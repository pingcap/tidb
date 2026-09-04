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

//! Session system and user variables.
//!
//! Go keeps every system variable in one process-wide registry
//! (`variable.GetSysVar`) holding a scope, a default value, and validation,
//! and each session keeps the values it has overridden. `SET` on a name the
//! registry does not know is `ErrUnknownSystemVar` (1193), and reading `@@x`
//! for an unknown name is `ErrUnknownSystemVariable` too.
//!
//! The registry itself is [`crate::sysvar`], which holds all 965 entries
//! captured from Go's own `GetSysVars()`, and the value validation Go's
//! `ValidateFromType` performs.
//!
//! GLOBAL scope: [`GlobalSysvars`] is a shared, `Arc`-backed table every
//! session a [`crate::Session`] factory opens holds a clone of -- Go's one
//! process-wide `GlobalVarAccessor` (kept in-memory here rather than
//! persisted to `mysql.GLOBAL_VARIABLES` in TiKV, so these globals reset on
//! process restart, exactly like [`crate::privilege::PrivilegeRegistry`] and
//! [`crate::process::ProcessRegistry`] do for accounts and connections).
//! `SET GLOBAL x = v` writes only into this shared table; the setting
//! session's own `@@x` is untouched (MySQL's rule: a live session keeps
//! whatever it already has). A brand new session snapshots the current
//! globals into its own session copy once, at connect
//! ([`SessionVars::seed_from_globals`]) -- so a session opened AFTER the
//! `SET GLOBAL` sees the new value as its session default, while sessions
//! already open do not.
//!
//! Per-variable effects that need the complete session remain at the session
//! layer: for example, `variables.rs` performs autocommit's OFF-to-ON commit,
//! while this module keeps Go's typed autocommit status in lockstep with the
//! normalized variable value.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use tidb_model::Job;
use tidb_planner::fix_control::OptimizerFixControl;

use crate::sysvar::{
    alias_of, get_sys_var, SysVarDef, ValidationError, SCOPE_GLOBAL, SCOPE_INSTANCE, SCOPE_SESSION,
};

/// Names a validation refusal the way the variable error surface reports it:
/// Go's `ErrWrongTypeForVar` (1232), `ErrWrongValueForVar` (1231) and the
/// bare `errors.Errorf` a `Validation` closure may raise.
///
/// One function rather than one per call site, because the three writers
/// (SESSION, GLOBAL/INSTANCE, and the read-only refusal in
/// `Session::check_max_allowed_packet_scope`) must agree: the same rejected
/// value has to name the same error whichever tier was being written.
pub(crate) fn validation_var_error(name: &str, value: &str, error: ValidationError) -> VarError {
    match error {
        ValidationError::WrongType => VarError::WrongTypeForVar(name.to_ascii_lowercase()),
        ValidationError::WrongValue => {
            VarError::WrongValueForVar(name.to_ascii_lowercase(), value.to_owned())
        }
        ValidationError::SqlError(error) => VarError::SqlError(error),
        ValidationError::Refused(message) => VarError::ValidationRefused(message),
    }
}

/// Expands Go's short `TypeTime` input for the two TTL schedule globals.
///
/// The registry validator is deliberately session-independent, so the
/// timezone-sensitive part lives at the GLOBAL write boundary where the
/// issuing [`SessionVars`] is available. Full values already carrying an
/// offset are preserved; short values use the current offset of the session
/// zone, just as Go's `time.ParseInLocation` does.
fn normalize_ttl_schedule_window(
    name: &str,
    value: &str,
    zone: &tidb_executor::SessionTimeZone,
) -> Result<String, VarError> {
    if !matches!(
        name.to_ascii_lowercase().as_str(),
        "tidb_ttl_job_schedule_window_start_time" | "tidb_ttl_job_schedule_window_end_time"
    ) {
        return Ok(value.to_owned());
    }
    let text = value.trim();
    let mut fields = text.split_whitespace();
    let clock = fields.next().unwrap_or_default();
    let explicit_offset = fields.next();
    if fields.next().is_some() || clock.is_empty() {
        return Err(VarError::ValidationRefused(format!(
            "invalid TTL job schedule window time: {value}"
        )));
    }
    let Some((hour, minute)) = clock.split_once(':') else {
        return Err(VarError::ValidationRefused(format!(
            "invalid TTL job schedule window time: {value}"
        )));
    };
    let (Ok(hour), Ok(minute)) = (hour.parse::<u8>(), minute.parse::<u8>()) else {
        return Err(VarError::ValidationRefused(format!(
            "invalid TTL job schedule window time: {value}"
        )));
    };
    if hour >= 24 || minute >= 60 {
        return Err(VarError::ValidationRefused(format!(
            "invalid TTL job schedule window time: {value}"
        )));
    }
    let offset_secs = if let Some(offset) = explicit_offset {
        parse_ttl_offset(offset).ok_or_else(|| {
            VarError::ValidationRefused(format!(
                "invalid TTL job schedule window time: {value}"
            ))
        })?
    } else {
        i32::try_from(zone.dag_zone().1).map_err(|_| {
            VarError::ValidationRefused(format!(
                "invalid TTL job schedule window time: {value}"
            ))
        })?
    };
    let sign = if offset_secs < 0 { '-' } else { '+' };
    let absolute = offset_secs.unsigned_abs();
    let offset_hours = absolute / 3600;
    let offset_minutes = (absolute % 3600) / 60;
    if offset_hours > 23 || offset_minutes > 59 {
        return Err(VarError::ValidationRefused(format!(
            "invalid TTL job schedule window time: {value}"
        )));
    }
    Ok(format!(
        "{hour:02}:{minute:02} {sign}{offset_hours:02}{offset_minutes:02}"
    ))
}

fn parse_ttl_offset(value: &str) -> Option<i32> {
    let bytes = value.as_bytes();
    if bytes.len() != 5 || !matches!(bytes[0], b'+' | b'-') {
        return None;
    }
    let hours = std::str::from_utf8(&bytes[1..3]).ok()?.parse::<i32>().ok()?;
    let minutes = std::str::from_utf8(&bytes[3..5]).ok()?.parse::<i32>().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let seconds = hours * 3600 + minutes * 60;
    Some(if bytes[0] == b'-' { -seconds } else { seconds })
}

/// The shared GLOBAL-scope value table every session of one
/// [`crate::Session`] factory holds a clone of. In-memory only: Go persists
/// this tier to `mysql.GLOBAL_VARIABLES`, so a real cluster survives a
/// restart with its `SET GLOBAL` values and this one does not -- the same
/// documented gap the account/privilege/process registries carry.
#[derive(Clone, Debug)]
pub struct GlobalSysvars {
    values: Arc<Mutex<HashMap<String, String>>>,
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
}

#[derive(Clone, Debug)]
enum InstanceMutation {
    Set(String, String),
    Reset(String),
}

impl Default for GlobalSysvars {
    fn default() -> Self {
        Self {
            values: Arc::default(),
            instances: Arc::default(),
            instance_mutations: None,
            publishes_runtime_settings: true,
            resolved: Arc::new(RwLock::new(EMPTY_RESOLVED.with(|it| it.clone()))),
        }
    }
}

impl tidb_executor::GlobalSysvarAccessor for GlobalSysvars {
    fn get_global_sysvar(&self, name: &str) -> Option<String> {
        self.get(name).ok()
    }
}

/// One read-mostly image of the node-wide variable tables.
///
/// [`GlobalSysvars::get`] used to answer through the authoritative
/// `Mutex<HashMap<String, String>>`: per statement execution that cost a lock,
/// a SipHash probe over a ~40-character name, and an owned String clone --
/// times the dozen-plus variables `Session::statement_context_ignoring`
/// re-reads every statement. Go pays none of that: its `SetSysVar` writes a
/// typed object once and every read is a field load. This image is the rust
/// equivalent at table granularity: rebuilt wholesale whenever a mutation
/// lands (`SET GLOBAL`, cluster loads), read lock-free otherwise, with each
/// slot holding an immutable `Arc<str>` so a read clones only the `Arc`.
/// Slot `i` mirrors registry entry `i`; the owning tier is static per
/// variable, so one flat table serves both maps.
#[derive(Debug, Clone)]
struct ResolvedGlobals {
    values: std::boxed::Box<[Option<Arc<str>>]>,
    /// Go's process-wide typed `vardef.OOMAction` atomic.
    oom_action: tidb_executor::OomAction,
    /// Go's process-wide typed `vardef.EnableTmpStorageOnOOM` atomic.
    tmp_storage_on_oom: bool,
}

impl Default for ResolvedGlobals {
    fn default() -> Self {
        Self {
            values: std::boxed::Box::default(),
            oom_action: tidb_executor::OomAction::Cancel,
            tmp_storage_on_oom: true,
        }
    }
}

impl ResolvedGlobals {
    /// Writes one slot by name; `None` records "at the registry default".
    fn note(&mut self, name: &str, value: Option<&str>) {
        if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
            if let Some(slot) = self.values.get_mut(index) {
                *slot = value.map(Arc::from);
            }
        }
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
    let lowered = crate::sysvar::lowered_if_needed(def.name);
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
        "tidb_record_plan_in_slow_log" => Some(instance.record_plan_in_slow_log.to_string()),
        "tidb_check_mb4_value_in_utf8" => Some(if instance.check_mb4_value_in_utf8.load() {
            "ON".to_owned()
        } else {
            "OFF".to_owned()
        }),
        "tidb_force_priority" => Some(instance.force_priority.clone()),
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

impl GlobalSysvars {
    /// A fresh registry with every variable at its default (Go's state on a
    /// cluster nobody has ever run `SET GLOBAL` against).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The tier a variable's node-wide value lives in. A variable that has
    /// GLOBAL scope at all is cluster state even when it ALSO has instance
    /// scope; only the instance-only ones are node-local.
    ///
    /// Every read and every write goes through here, so a value can never be
    /// written to one tier and looked for in the other -- the failure mode
    /// that makes a `SET` a silent no-op.
    fn store(&self, def: &'static SysVarDef) -> &Arc<Mutex<HashMap<String, String>>> {
        if def.has_global_scope() {
            &self.values
        } else {
            &self.instances
        }
    }

    /// Reads a node-wide value (GLOBAL or INSTANCE tier), falling back to the
    /// registry default.
    ///
    /// The answer comes from the [`ResolvedGlobals`] image when it is current;
    /// only a mutation that has not been published yet falls through to the
    /// authoritative maps.
    pub fn get(&self, name: &str) -> Result<String, VarError> {
        let Some(index) = crate::sysvar::sys_var_index_lookup(name) else {
            return Err(VarError::UnknownSystemVariable(name.to_ascii_lowercase()));
        };
        let def = &crate::sysvar::SYS_VARS[index];
        if self.publishes_runtime_settings && crate::embedding::is_embedding_variable(def.name) {
            return Ok(crate::embedding::masked_global_value(def.name)
                .expect("embedding variable has a process-wide value"));
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
        if snapshot.values.len() == crate::sysvar::SYS_VARS.len() {
            // The image is current (a mutation republishes it before releasing
            // its writer), so an empty slot means "at its registry default".
            return Ok(crate::sysvar::effective_default(def));
        }
        // A pre-image from `Default` (or a racing rebuild): answer from the
        // authoritative maps, the way every caller saw before this cache.
        let lowered = crate::sysvar::lowered_if_needed(name);
        Ok(self
            .store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .get(lowered.as_ref())
            .cloned()
            .unwrap_or_else(|| crate::sysvar::effective_default(def)))
    }

    /// Rebuilds the read-mostly image from the two authoritative maps. Every
    /// mutating entry point calls this after releasing its map lock; readers
    /// swap in the new `Arc` wholesale.
    fn refresh_resolved(&self) {
        let mut slots: Vec<Option<Arc<str>>> =
            std::vec::Vec::with_capacity(crate::sysvar::SYS_VARS.len());
        slots.resize_with(crate::sysvar::SYS_VARS.len(), || None);
        for (name, value) in self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .iter()
        {
            if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
                if crate::sysvar::SYS_VARS[index].has_global_scope() {
                    slots[index] = Some(Arc::from(value.as_str()));
                }
            }
        }
        for (name, value) in self
            .instances
            .lock()
            .expect("instance sysvar lock poisoned")
            .iter()
        {
            if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
                if !crate::sysvar::SYS_VARS[index].has_global_scope() {
                    slots[index] = Some(Arc::from(value.as_str()));
                }
            }
        }
        let effective = |name: &str| {
            let index = crate::sysvar::sys_var_index_lookup(name)
                .expect("typed global policy names are registered");
            slots[index]
                .as_deref()
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    crate::sysvar::effective_default(&crate::sysvar::SYS_VARS[index])
                })
        };
        let oom_action = tidb_executor::OomAction::parse(&effective(
            tidb_vardef::tidb_vars::TIDB_MEM_OOM_ACTION,
        ));
        let oom_action_text = effective(tidb_vardef::tidb_vars::TIDB_MEM_OOM_ACTION);
        let tmp_storage = effective(tidb_vardef::tidb_vars::TIDB_ENABLE_TMP_STORAGE_ON_OOM);
        let tmp_storage_on_oom = !(tmp_storage.eq_ignore_ascii_case("off") || tmp_storage == "0");
        let memory_usage_alarm_ratio =
            effective(tidb_vardef::tidb_vars::TIDB_MEMORY_USAGE_ALARM_RATIO)
                .parse::<f64>()
                .expect("validated memory usage alarm ratio is a float");
        let memory_usage_alarm_keep_record_num =
            effective(tidb_vardef::tidb_vars::TIDB_MEMORY_USAGE_ALARM_KEEP_RECORD_NUM)
                .parse::<i64>()
                .expect("validated memory usage alarm record count is an integer");
        let analyze_default_num_buckets =
            effective(tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_BUCKETS)
                .parse::<u64>()
                .expect("validated analyze bucket default is an unsigned integer");
        let analyze_default_num_top_n =
            effective(tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_TOP_N)
                .parse::<u64>()
                .expect("validated analyze TopN default is an unsigned integer");
        let stats_cache_mem_quota = effective(tidb_vardef::tidb_vars::TIDB_STATS_CACHE_MEM_QUOTA)
            .parse::<i64>()
            .expect("validated statistics cache quota is an integer");
        let circuit_breaker_pd_metadata_error_rate_threshold_ratio = effective(
            tidb_vardef::tidb_vars::TIDB_CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO,
        )
        .parse::<f64>()
        .unwrap_or(tidb_vardef::defaults::DEF_TIDB_CIRCUIT_BREAKER_PD_META_ERROR_RATE_RATIO);
        let mut publish = self
            .resolved
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *publish = Arc::new(ResolvedGlobals {
            values: slots.into(),
            oom_action,
            tmp_storage_on_oom,
        });
        if self.publishes_runtime_settings {
            tidb_vardef::set_oom_action(&oom_action_text);
            tidb_vardef::set_memory_usage_alarm_ratio(memory_usage_alarm_ratio);
            tidb_vardef::MEMORY_USAGE_ALARM_KEEP_RECORD_NUM.store(
                memory_usage_alarm_keep_record_num,
                std::sync::atomic::Ordering::SeqCst,
            );
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.store(
                analyze_default_num_buckets,
                std::sync::atomic::Ordering::SeqCst,
            );
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.store(
                analyze_default_num_top_n,
                std::sync::atomic::Ordering::SeqCst,
            );
            tidb_vardef::STATS_CACHE_MEM_QUOTA
                .store(stats_cache_mem_quota, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::set_circuit_breaker_pd_metadata_error_rate_threshold_ratio(
                circuit_breaker_pd_metadata_error_rate_threshold_ratio,
            );
        }
    }

    /// The typed process-wide statement-memory policy Go exposes through
    /// `vardef.OOMAction` and `vardef.EnableTmpStorageOnOOM` atomics.
    ///
    /// It is parsed when a GLOBAL mutation publishes the resolved image, not
    /// when each statement starts.
    pub(crate) fn statement_memory_policy(&self) -> (tidb_executor::OomAction, bool) {
        let snapshot = Arc::clone(
            &*self
                .resolved
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        (snapshot.oom_action, snapshot.tmp_storage_on_oom)
    }

    /// Reads one variable by its registry position, skipping the name probe
    /// callers already paid (`SessionVars::get_system` resolves the index
    /// once for both tiers). Same image-then-authoritative fallback as
    /// [`Self::get`].
    pub(crate) fn get_by_registry_index(&self, index: usize) -> Result<String, VarError> {
        let def = &crate::sysvar::SYS_VARS[index];
        if self.publishes_runtime_settings && crate::embedding::is_embedding_variable(def.name) {
            return Ok(crate::embedding::masked_global_value(def.name)
                .expect("embedding variable has a process-wide value"));
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
        if snapshot.values.len() == crate::sysvar::SYS_VARS.len() {
            return Ok(crate::sysvar::effective_default(def));
        }
        let lowered = crate::sysvar::lowered_if_needed(def.name);
        Ok(self
            .store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .get(lowered.as_ref())
            .cloned()
            .unwrap_or_else(|| crate::sysvar::effective_default(def)))
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
        self.set_with_time_zone(name, value, &tidb_executor::SessionTimeZone::utc())
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
        zone: &tidb_executor::SessionTimeZone,
    ) -> Result<bool, VarError> {
        let value = normalize_ttl_schedule_window(name, &value, zone)?;
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
    /// it replaces the registry default directly, with no scope or read-only
    /// validation, which is how read-only NONE-scope variables like `port`,
    /// `socket` and `hostname` get their per-process values. Runtime `SET`
    /// statements never come through here.
    pub fn set_startup(&self, name: &str, value: String) {
        let Some(def) = get_sys_var(name) else {
            return;
        };
        self.store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .insert(name.to_ascii_lowercase(), value);
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT) {
            self.publish_require_secure_transport();
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE) {
            self.publish_ttl_job_enable();
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE) {
            if let Ok(value) = self.get(tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE) {
                self.publish_schema_cache_size(&value);
            }
        }
        self.publish_embedding_settings();
        self.refresh_resolved();
    }

    /// Publishes the process-wide embedding settings after a live table
    /// mutation. Scratch cluster tables deliberately keep their values in the
    /// table until `replace_from` makes the committed image live.
    fn publish_embedding_settings(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let names = [
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_JINA_AI_API_KEY,
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_OPENAI_API_KEY,
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE,
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_COHERE_API_KEY,
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_HUGGINGFACE_API_KEY,
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_NVIDIA_NIM_API_KEY,
            tidb_vardef::tidb_vars::TIDB_EXP_EMBED_GEMINI_API_KEY,
        ];
        let values = self.values.lock().expect("global sysvar lock poisoned");
        for name in names {
            crate::embedding::publish_global(name, values.get(name).map(String::as_str));
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
        let validated = def
            .validate_in_scope(&value, scope)
            .map_err(|error| validation_var_error(name, &value, error))?;
        let key = name.to_ascii_lowercase();
        if key == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL {
            OptimizerFixControl::parse(&validated.value)
                .map_err(|error| VarError::ValidationRefused(error.to_string()))?;
        }
        let stored_value = validated.value;
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
        if matches!(
            key.as_str(),
            "validate_password.mixed_case_count"
                | "validate_password.number_count"
                | "validate_password.special_char_count"
        ) {
            // Setting a count raises the stored `length` to the new minimum
            // when the current length falls short (`updatePasswordValidationLength`,
            // `varsutil.go:446`): a plain store, no further validation.
            if let Some(required) = self.validate_password_required_length() {
                let length_def = get_sys_var("validate_password.length");
                if let Some(length_def) = length_def {
                    let mut values = self
                        .store(length_def)
                        .lock()
                        .expect("global sysvar lock poisoned");
                    let current: i64 = values
                        .get("validate_password.length")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(8);
                    if current < required {
                        values.insert("validate_password.length".to_owned(), required.to_string());
                    }
                }
            }
        }
        self.publish_embedding_settings();
        if key == tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT {
            self.publish_require_secure_transport();
        }
        if key == tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE {
            self.publish_ttl_job_enable();
        }
        if key == tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE {
            self.publish_schema_cache_size(&stored_value);
        }
        self.refresh_resolved();
        if key == tidb_vardef::tidb_vars::TIDB_REDACT_LOG {
            self.publish_redaction_mode();
        }
        if !def.has_global_scope() {
            self.record_instance_mutation(InstanceMutation::Set(key.clone(), stored_value));
        }
        if key == tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY {
            self.publish_committer_concurrency();
        }
        self.publish_memory_arbitration_setting(&key);
        Ok(validated.truncated)
    }

    /// Publishes Go's `vardef.EnableTTLJob` process-wide switch from the
    /// live GLOBAL table. Scratch registries deliberately skip this hook and
    /// publish it only when their committed image replaces the live table.
    fn publish_ttl_job_enable(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let enabled = self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .get(tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE)
            .map_or(tidb_vardef::defaults::DEF_TIDB_TTL_JOB_ENABLE, |value| {
                value.eq_ignore_ascii_case("ON") || value == "1"
            });
        tidb_vardef::ENABLE_TTL_JOB.store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    /// Publishes Go's `vardef.SchemaCacheSize` byte counter from a validated
    /// origin string. The origin spelling remains the SQL-facing value in the
    /// global table; this typed product is what the infoschema cache consumes.
    fn publish_schema_cache_size(&self, value: &str) {
        let bytes = crate::varsutil::parse_byte_size(value)
            .map(|(bytes, _)| bytes)
            .or_else(|| value.parse::<u64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE as u64);
        tidb_vardef::SCHEMA_CACHE_SIZE.store(bytes, std::sync::atomic::Ordering::SeqCst);
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

    /// Restores the registry default (`SET GLOBAL x = DEFAULT`).
    pub fn reset(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        let key = name.to_ascii_lowercase();
        self.store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .remove(&key);
        self.publish_embedding_settings();
        if key == tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT {
            self.publish_require_secure_transport();
        }
        if key == tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE {
            self.publish_ttl_job_enable();
        }
        if key == tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE {
            self.publish_schema_cache_size(
                tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE
                    .to_string()
                    .as_str(),
            );
        }
        self.refresh_resolved();
        if !def.has_global_scope() {
            self.record_instance_mutation(InstanceMutation::Reset(key.clone()));
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY) {
            self.publish_committer_concurrency();
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_REDACT_LOG) {
            self.publish_redaction_mode();
        }
        self.publish_memory_arbitration_setting(&key);
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
        let key = name.to_ascii_lowercase();
        self.store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .remove(&key);
        self.refresh_resolved();
        if !def.has_global_scope() {
            self.record_instance_mutation(InstanceMutation::Reset(key));
        }
        self.publish_memory_arbitration_setting(name);
        Ok(())
    }

    /// Overwrites this table's values from a cluster's stored
    /// `mysql.global_variables` rows, ahead of any session opening.
    ///
    /// Unlike [`Self::set`], this does not validate: a stored row already
    /// passed that validation when some earlier `SET GLOBAL` (Go's or this
    /// node's own) wrote it, so re-validating it here could only reject a
    /// value this node's own registry has since drifted from. A name this
    /// registry does not recognize is skipped rather than refused, the same
    /// forward/backward-compatibility stance `tidb_exec::cluster_privilege_load`
    /// takes on a column or privilege name the running version does not
    /// know.
    pub fn load_from_cluster<I: IntoIterator<Item = (String, String)>>(&self, rows: I) {
        let mut loaded_committer_concurrency = false;
        let mut loaded_redaction_mode = false;
        let mut loaded_memory_arbitration = false;
        let mut loaded_require_secure_transport = false;
        let mut loaded_ttl_job_enable = false;
        let mut loaded_schema_cache_size = false;
        for (name, value) in rows {
            let key = name.to_ascii_lowercase();
            if let Some(def) = get_sys_var(&key) {
                loaded_committer_concurrency |=
                    key == tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY;
                loaded_redaction_mode |= key == tidb_vardef::tidb_vars::TIDB_REDACT_LOG;
                loaded_memory_arbitration |= Self::is_memory_arbitration_setting(&key);
                loaded_require_secure_transport |=
                    key == tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT;
                loaded_ttl_job_enable |= key == tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE;
                loaded_schema_cache_size |= key == tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE;
                self.store(def)
                    .lock()
                    .expect("global sysvar lock poisoned")
                    .insert(key, value);
            }
        }
        if loaded_committer_concurrency {
            self.publish_committer_concurrency();
        }
        if loaded_redaction_mode {
            self.publish_redaction_mode();
        }
        if loaded_memory_arbitration {
            self.publish_memory_arbitration_settings();
        }
        if loaded_require_secure_transport {
            self.publish_require_secure_transport();
        }
        if loaded_ttl_job_enable {
            self.publish_ttl_job_enable();
        }
        if loaded_schema_cache_size {
            if let Ok(value) = self.get(tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE) {
                self.publish_schema_cache_size(&value);
            }
        }
        self.publish_embedding_settings();
        self.refresh_resolved();
    }

    /// Every variable this table currently overrides from its default, for
    /// [`SessionVars::seed_from_globals`] and `SHOW GLOBAL VARIABLES`.
    pub fn overrides(&self) -> HashMap<String, String> {
        self.values
            .lock()
            .expect("global sysvar lock poisoned")
            .clone()
    }

    /// A fresh table seeded from a cluster's stored
    /// `mysql.global_variables` rows -- the scratch copy a convergence node
    /// validates one `SET GLOBAL` against before persisting it, the same
    /// shape [`crate::privilege::PrivilegeRegistry`]'s cluster scratch table
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

    /// Publishes `fresh`'s GLOBAL values into every existing clone of this
    /// table while retaining this node's INSTANCE-only overrides.
    ///
    /// Mirrors [`crate::privilege::PrivilegeRegistry::replace_from`]: this
    /// `mysql.global_variables` never contains the process-local INSTANCE
    /// tier. A startup or periodic cluster rebuild must therefore replace the
    /// persisted GLOBAL image without clearing settings written through
    /// `SET INSTANCE` on this node.
    pub fn replace_from(&self, fresh: &Self) {
        *self.values.lock().expect("global sysvar lock poisoned") =
            std::mem::take(&mut *fresh.values.lock().expect("global sysvar lock poisoned"));
        self.refresh_resolved();
        self.publish_require_secure_transport();
        self.publish_ttl_job_enable();
        if let Ok(value) = self.get(tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE) {
            self.publish_schema_cache_size(&value);
        }
        self.publish_committer_concurrency();
        self.publish_redaction_mode();
        self.publish_memory_arbitration_settings();
        self.publish_embedding_settings();
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

    fn publish_committer_concurrency(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let value = self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .get(tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY)
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(
                i32::try_from(tidb_vardef::defaults::DEF_TIDB_COMMITTER_CONCURRENCY)
                    .expect("committer concurrency default fits i32"),
            );
        tidb_tikvutil::COMMITTER_CONCURRENCY.store(value, std::sync::atomic::Ordering::SeqCst);
    }

    fn publish_require_secure_transport(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let enabled = if tidb_config::deploymode::is_starter() {
            false
        } else {
            self.values
                .lock()
                .expect("global sysvar lock poisoned")
                .get(tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT)
                .is_some_and(|value| value == "ON" || value == "1")
        };
        tidb_util::tls::REQUIRE_SECURE_TRANSPORT
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    fn publish_redaction_mode(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let value = self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .get(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
            .cloned()
            .unwrap_or_else(|| {
                crate::sysvar::effective_default(
                    get_sys_var(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
                        .expect("tidb_redact_log is registered"),
                )
            });
        let mode = match value.as_str() {
            "ON" => tidb_error::mysql::RedactionMode::Enabled,
            "MARKER" => tidb_error::mysql::RedactionMode::Marker,
            _ => tidb_error::mysql::RedactionMode::Disabled,
        };
        tidb_error::mysql::set_redaction_mode(mode);
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

    fn publish_memory_arbitration_setting(&self, name: &str) {
        if self.publishes_runtime_settings && Self::is_memory_arbitration_setting(name) {
            if let Ok(value) = self.get(name) {
                let _ = tidb_util::memory::apply_process_memory_setting(name, &value);
            }
        }
    }

    fn publish_memory_arbitration_settings(&self) {
        for name in [
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE,
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE,
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT,
        ] {
            self.publish_memory_arbitration_setting(name);
        }
    }
}

/// Why a variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarError {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrVariableNoLongerSupported` (8136), used by the expression
    /// read path for a removed system variable.
    RemovedSystemVariable { name: String, reason: String },
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
    /// Go `ErrWrongTypeForVar` (1232).
    WrongTypeForVar(String),
    /// Go `ErrWrongValueForVar` (1231).
    WrongValueForVar(String, String),
    /// A catalogued MySQL error returned unchanged by validation.
    SqlError(tidb_error::mysql::SqlError),
    /// Go `ErrLocalVariable` (1228): `SET GLOBAL` named a SESSION-only
    /// variable.
    SessionOnlyVariable(String),
    /// Go `ErrGlobalVariable` (1229): `SET SESSION` named a GLOBAL-only
    /// variable.
    GlobalOnlyVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238), the read-side form: `SELECT
    /// @@global.x` named a SESSION-only variable (there is no GLOBAL copy to
    /// read).
    NoGlobalCopy(String),
    /// Go `ErrIncorrectScope` (1238), the expression rewriter's explicit read
    /// scope validation. The second field is Go's allowed-scope text, such as
    /// `GLOBAL` or `SESSION or GLOBAL`.
    IncorrectScope(String, &'static str),
    /// A `SysVar.Validation` closure's own `errors.Errorf`, whose wording is
    /// the whole error (Go gives it no code, so it reports as 1105).
    ValidationRefused(String),
}

/// A session's system-variable state: the variables it has overridden (Go's
/// `SessionVars.systems`) plus the shared GLOBAL table.
///
/// User variables are NOT here. They live in one place -- the shared map
/// [`crate::Session`] lends to every statement context -- because `@x := expr`
/// writes them from inside expression evaluation, mid-row; see
/// `tidb_executor::StmtContext`'s `user_vars` field.
#[derive(Clone, Debug)]
pub struct SessionVars {
    systems: HashMap<String, String>,
    /// Go's typed `ServerStatusAutocommit` bit.
    ///
    /// The normalized system-variable text remains the SQL read authority;
    /// transaction, planner-cache, process-list, and wire-status consumers
    /// read this field exactly as Go reads `SessionVars.status`.
    autocommit: bool,
    /// Go's typed `SessionVars.SQLMode`, maintained by the sql_mode sysvar's
    /// `SetSession` hook and read directly by parser and executor consumers.
    sql_mode: tidb_mysql::SqlMode,
    /// Go's typed `SessionVars.MaxAllowedPacket`, maintained by the sysvar's
    /// `SetSession` hook and read directly by wire and builtin consumers.
    max_allowed_packet: u64,
    /// Go's typed `SessionVars.MaxKeysRead`, maintained by the
    /// `tidb_max_keys_read` `SetSession` hook. The Go accessor returns this
    /// value only while a SELECT statement is active; callers pass that
    /// statement-shape bit to [`Self::max_keys_read`].
    max_keys_read: u64,
    /// Go's typed `SessionVars.MaxExecutionTime`, maintained by the
    /// `max_execution_time` `SetSession` hook and read by statement contexts
    /// as a millisecond deadline (zero means unlimited).
    max_execution_time: u64,
    /// Go's typed `SessionVars.TimeZone`, resolved once by the time-zone
    /// `SetSession` hook rather than reparsed by every statement.
    time_zone: tidb_executor::SessionTimeZone,
    /// Go's typed `SessionVars.SelectLimit`, maintained by the
    /// `sql_select_limit` `SetSession` hook. `u64::MAX` is the unlimited
    /// default and any smaller value caps top-level SELECT/set results.
    select_limit: u64,
    /// Go's typed `SessionVars.MultiStatementMode`: OFF=0, ON=1, WARN=2.
    /// The normalized enum value drives COM_QUERY multi-statement admission.
    multi_statement_mode: u8,
    /// Go's typed `SessionVars.EnablePreparedPlanCache`, maintained by the
    /// `tidb_enable_prepared_plan_cache` sysvar's `SetSession` hook.
    enable_prepared_plan_cache: bool,
    /// Go's typed `SessionVars.EnableSharedLockUpgrade`, maintained by the
    /// `tidb_enable_shared_lock_upgrade` sysvar's `SetSession` hook.
    enable_shared_lock_upgrade: bool,
    /// Go's typed `SessionVars.EnableWindowFunction`, maintained by the
    /// `tidb_enable_window_function` sysvar's `SetSession` hook.
    enable_window_function: bool,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalJoin`, maintained
    /// by the corresponding TiFlash threshold `SetSession` hook.
    ti_flash_max_bytes_before_ext_join: i64,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalGroupBy`.
    ti_flash_max_bytes_before_ext_agg: i64,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalSort`.
    ti_flash_max_bytes_before_ext_sort: i64,
    /// Go's typed `SessionVars.TiFlashMemQuotaQueryPerNode`.
    ti_flash_mem_quota_query_per_node: i64,
    /// Go's typed `SessionVars.TiFlashQuerySpillRatio`.
    ti_flash_query_spill_ratio: f64,
    /// Go's typed `SessionVars.PessimisticTransactionFairLocking`.
    pessimistic_transaction_fair_locking: bool,
    /// Go's typed `SessionVars.BulkDMLEnabled`, maintained by
    /// `tidb_dml_type`'s SetSession hook.
    bulk_dml_enabled: bool,
    /// Go's typed replica-read selection, maintained by the
    /// `tidb_replica_read` SetSession hook.
    replica_read: tidb_executor::ReplicaReadType,
    /// Bumped by every mutation of `systems`, so a caller can cache what it
    /// PARSES out of the raw text -- chiefly the optimizer's cost environment
    /// -- and re-derive only when a `SET`
    /// actually happened. Go holds the same products as typed fields on
    /// `SessionVars` updated by each variable's `SetSession` hook; a
    /// generation stamp buys that read cost without a hook per variable.
    /// Session-scoped reads never consult the shared globals
    /// (`get_system`'s fallback is the static default), so this counter
    /// alone is a complete invalidation key for them.
    generation: u64,
    /// Parsed authority kept in lockstep with the raw system-variable text.
    optimizer_fix_control: OptimizerFixControl,
    /// The session-tier read-mostly image (`ResolvedGlobals`, same shape as
    /// the global one): rebuilt whenever `systems` mutates, consulted by
    /// [`Self::get_system`] ahead of the authoritative map so a statement's
    /// dozens of variable reads cost one fixed-seed probe plus an `Arc` slot
    /// check instead of a SipHash probe and a String clone apiece.
    session_resolved: ResolvedGlobals,
    /// The shared GLOBAL-scope table this session's factory holds. Cloning a
    /// [`GlobalSysvars`] is cheap (one `Arc` bump), so every session shares
    /// the same underlying map.
    globals: Arc<GlobalSysvars>,
    /// Go `SessionVars.InMViewMaintenance`: set programmatically (not via a
    /// sysvar) while the session executes internal MV build/refresh
    /// statements.
    in_mview_maintenance: bool,
}

/// Resolves the validated `time_zone` text into the statement-facing zone
/// type. Go's `timeutil.ParseTimeZone` preserves the original `+HH:MM` name
/// on the session location even though the DAGR request later sends an empty
/// name for fixed offsets; retaining that text here keeps both behaviors.
fn resolve_session_time_zone_value(written: &str) -> tidb_executor::SessionTimeZone {
    use tidb_executor::SessionTimeZone;

    if !written.eq_ignore_ascii_case("SYSTEM") {
        if let Ok(zone) = written.parse::<chrono_tz::Tz>() {
            return SessionTimeZone::Named(zone);
        }
        if let Some(rest) = written.strip_prefix(['+', '-']) {
            let negative = written.starts_with('-');
            let mut parts = rest.split(':');
            let hours: i32 = parts.next().unwrap_or_default().parse().unwrap_or(-1);
            let minutes: i32 = parts.next().unwrap_or("0").parse().unwrap_or(-1);
            if hours >= 0 && (0..60).contains(&minutes) {
                let offset = hours * 3600 + minutes * 60;
                let bounded = if negative {
                    offset <= 12 * 3600 + 59 * 60
                } else {
                    offset <= 14 * 3600
                };
                if bounded {
                    return SessionTimeZone::Fixed {
                        name: written.to_owned(),
                        offset_secs: if negative { -offset } else { offset },
                    };
                }
            }
        }
    }

    // SYSTEM is TiDB's process-wide SystemLocation, not an offset snapshot.
    // Preserve a resolved IANA zone (and therefore DST), with the process
    // local zone as the same fallback Go uses.
    match tidb_util::timeutil::system_location() {
        tidb_util::timeutil::TimeZone::Local => SessionTimeZone::Local,
        tidb_util::timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
        tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } => {
            SessionTimeZone::Fixed { name, offset_secs }
        }
    }
}

impl Default for SessionVars {
    fn default() -> Self {
        Self {
            systems: HashMap::new(),
            autocommit: true,
            sql_mode: tidb_mysql::get_sql_mode(tidb_mysql::DefaultSQLMode)
                .expect("the compiled default SQL mode is valid"),
            max_allowed_packet: 64 << 20,
            max_keys_read: 0,
            max_execution_time: 0,
            time_zone: resolve_session_time_zone_value("SYSTEM"),
            select_limit: u64::MAX,
            multi_statement_mode: 0,
            enable_prepared_plan_cache: tidb_vardef::defaults::DEF_TIDB_ENABLE_PREP_PLAN_CACHE,
            enable_shared_lock_upgrade: tidb_vardef::defaults::DEF_TIDB_ENABLE_SHARED_LOCK_UPGRADE,
            enable_window_function: tidb_vardef::defaults::DEF_ENABLE_WINDOW_FUNCTION,
            ti_flash_max_bytes_before_ext_join:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ti_flash_max_bytes_before_ext_agg:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY,
            ti_flash_max_bytes_before_ext_sort:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_SORT,
            ti_flash_mem_quota_query_per_node:
                tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            ti_flash_query_spill_ratio: tidb_vardef::defaults::DEF_TIFLASH_QUERY_SPILL_RATIO,
            pessimistic_transaction_fair_locking: false,
            bulk_dml_enabled: false,
            replica_read: tidb_executor::ReplicaReadType::Leader,
            generation: 0,
            optimizer_fix_control: OptimizerFixControl::default(),
            session_resolved: ResolvedGlobals::default(),
            globals: Arc::default(),
            in_mview_maintenance: false,
        }
    }
}

impl SessionVars {
    /// Go `SessionVars.InMViewMaintenance` read: whether the session is
    /// executing internal MV build/refresh statements.
    #[must_use]
    pub fn in_mview_maintenance(&self) -> bool {
        self.in_mview_maintenance
    }

    /// Go `SessionVars.InMViewMaintenance` write.
    pub fn set_in_mview_maintenance(&mut self, value: bool) {
        self.in_mview_maintenance = value;
    }

    /// A session with every variable at its registry default and its own,
    /// unshared global table (used by tests and any standalone session that
    /// has no factory to share one with).
    #[must_use]
    pub fn new() -> Self {
        SessionVars::default()
    }

    /// Points this session at a shared [`GlobalSysvars`] table and snapshots
    /// its current overrides into the session's own copy -- Go's rule that a
    /// session's variables are copied from the global tier once, at connect.
    /// A `SET GLOBAL` another session runs AFTER this call is invisible to
    /// this session's plain `@@x` (only to `@@global.x`), exactly as MySQL
    /// documents.
    pub fn seed_from_globals(&mut self, globals: GlobalSysvars) -> Result<(), VarError> {
        let mut systems = self.systems.clone();
        for (name, value) in globals.overrides() {
            // Only a variable this session can actually hold a session copy
            // of inherits the global value; Go's `NewSessionVars` walks the
            // same `HasSessionScope` guard when copying `GlobalVarsAccessor`
            // into a fresh session.
            if get_sys_var(&name).is_some_and(|def| def.has_session_scope()) {
                systems.insert(name, value);
            }
        }
        let raw = systems
            .get(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
            .cloned()
            .unwrap_or_else(|| {
                get_sys_var(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
                    .map_or_else(String::new, |def| def.value.to_owned())
            });
        let optimizer_fix_control = OptimizerFixControl::parse(&raw)
            .map_err(|error| VarError::ValidationRefused(error.to_string()))?
            .0;
        let autocommit = Self::autocommit_from_systems(&systems);
        let sql_mode = Self::sql_mode_from_systems(&systems)
            .map_err(|error| VarError::ValidationRefused(error.to_string()))?;
        let max_allowed_packet = Self::max_allowed_packet_from_systems(&systems)?;
        let max_keys_read = Self::max_keys_read_from_systems(&systems);
        let max_execution_time = Self::max_execution_time_from_systems(&systems);
        let time_zone = Self::time_zone_from_systems(&systems);
        let select_limit = Self::select_limit_from_systems(&systems);
        let multi_statement_mode = Self::multi_statement_mode_from_systems(&systems);
        let enable_prepared_plan_cache = Self::prepared_plan_cache_from_systems(&systems);
        let enable_shared_lock_upgrade = Self::shared_lock_upgrade_from_systems(&systems);
        let enable_window_function = Self::enable_window_function_from_systems(&systems);
        let ti_flash_max_bytes_before_ext_join =
            Self::ti_flash_max_bytes_before_ext_join_from_systems(&systems);
        let ti_flash_max_bytes_before_ext_agg =
            Self::ti_flash_max_bytes_before_ext_agg_from_systems(&systems);
        let ti_flash_max_bytes_before_ext_sort =
            Self::ti_flash_max_bytes_before_ext_sort_from_systems(&systems);
        let ti_flash_mem_quota_query_per_node =
            Self::ti_flash_mem_quota_query_per_node_from_systems(&systems);
        let ti_flash_query_spill_ratio = Self::ti_flash_query_spill_ratio_from_systems(&systems);
        let pessimistic_transaction_fair_locking =
            Self::pessimistic_transaction_fair_locking_from_systems(&systems);
        let bulk_dml_enabled = Self::bulk_dml_enabled_from_systems(&systems);
        let replica_read = Self::replica_read_from_systems(&systems);
        // Commit all authorities only after the inherited fix-control
        // row has been accepted. A stale/foreign cluster row can therefore
        // refuse the connection without partially reseeding this session.
        self.systems = systems;
        self.globals = Arc::new(globals);
        self.optimizer_fix_control = optimizer_fix_control;
        self.autocommit = autocommit;
        self.sql_mode = sql_mode;
        self.max_allowed_packet = max_allowed_packet;
        self.max_keys_read = max_keys_read;
        self.max_execution_time = max_execution_time;
        self.time_zone = time_zone;
        self.select_limit = select_limit;
        self.multi_statement_mode = multi_statement_mode;
        self.enable_prepared_plan_cache = enable_prepared_plan_cache;
        self.enable_shared_lock_upgrade = enable_shared_lock_upgrade;
        self.enable_window_function = enable_window_function;
        self.ti_flash_max_bytes_before_ext_join = ti_flash_max_bytes_before_ext_join;
        self.ti_flash_max_bytes_before_ext_agg = ti_flash_max_bytes_before_ext_agg;
        self.ti_flash_max_bytes_before_ext_sort = ti_flash_max_bytes_before_ext_sort;
        self.ti_flash_mem_quota_query_per_node = ti_flash_mem_quota_query_per_node;
        self.ti_flash_query_spill_ratio = ti_flash_query_spill_ratio;
        self.pessimistic_transaction_fair_locking = pessimistic_transaction_fair_locking;
        self.bulk_dml_enabled = bulk_dml_enabled;
        self.replica_read = replica_read;
        self.session_resolved = Self::build_session_image(&self.systems);
        // The wholesale replacement above is a mutation like any other; the
        // parsed-product caches keyed on `generation` must not survive it.
        self.generation += 1;
        Ok(())
    }

    fn autocommit_from_systems(systems: &HashMap<String, String>) -> bool {
        systems.get("autocommit").map_or(true, |value| {
            value.eq_ignore_ascii_case("ON") || value == "1"
        })
    }

    fn sql_mode_from_systems(
        systems: &HashMap<String, String>,
    ) -> Result<tidb_mysql::SqlMode, tidb_mysql::InvalidSqlMode> {
        tidb_mysql::get_sql_mode(
            systems
                .get("sql_mode")
                .map_or(tidb_mysql::DefaultSQLMode, String::as_str),
        )
    }

    fn max_allowed_packet_from_systems(systems: &HashMap<String, String>) -> Result<u64, VarError> {
        systems
            .get("max_allowed_packet")
            .map_or("67108864", String::as_str)
            .parse::<u64>()
            .map_err(|error| VarError::ValidationRefused(error.to_string()))
    }

    fn max_keys_read_from_systems(systems: &HashMap<String, String>) -> u64 {
        systems
            .get("tidb_max_keys_read")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn max_execution_time_from_systems(systems: &HashMap<String, String>) -> u64 {
        systems
            .get("max_execution_time")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn time_zone_from_systems(systems: &HashMap<String, String>) -> tidb_executor::SessionTimeZone {
        resolve_session_time_zone_value(systems.get("time_zone").map_or("SYSTEM", String::as_str))
    }

    fn select_limit_from_systems(systems: &HashMap<String, String>) -> u64 {
        systems
            .get("sql_select_limit")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(u64::MAX)
    }

    fn multi_statement_mode_from_systems(systems: &HashMap<String, String>) -> u8 {
        match systems.get("tidb_multi_statement_mode").map(String::as_str) {
            Some("ON") => 1,
            Some("WARN") => 2,
            _ => 0,
        }
    }

    fn prepared_plan_cache_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE)
            .map_or(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_PREP_PLAN_CACHE,
                |value| value == "ON",
            )
    }

    fn shared_lock_upgrade_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE)
            .map_or(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_SHARED_LOCK_UPGRADE,
                |value| value == "ON",
            )
    }

    fn enable_window_function_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_WINDOW_FUNCTION)
            .map_or(tidb_vardef::defaults::DEF_ENABLE_WINDOW_FUNCTION, |value| {
                value.eq_ignore_ascii_case("ON") || value == "1"
            })
    }

    fn ti_flash_max_bytes_before_ext_join_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN)
    }

    fn ti_flash_max_bytes_before_ext_agg_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY)
    }

    fn ti_flash_max_bytes_before_ext_sort_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_SORT)
    }

    fn ti_flash_mem_quota_query_per_node_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE)
    }

    fn ti_flash_query_spill_ratio_from_systems(systems: &HashMap<String, String>) -> f64 {
        systems
            .get(tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_QUERY_SPILL_RATIO)
    }

    fn pessimistic_transaction_fair_locking_from_systems(
        systems: &HashMap<String, String>,
    ) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING)
            .is_some_and(|value| value.eq_ignore_ascii_case("ON") || value == "1")
    }

    fn bulk_dml_enabled_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_DML_TYPE)
            .is_some_and(|value| value.eq_ignore_ascii_case("bulk"))
    }

    fn replica_read_from_systems(
        systems: &HashMap<String, String>,
    ) -> tidb_executor::ReplicaReadType {
        match systems
            .get(tidb_vardef::tidb_vars::TIDB_REPLICA_READ)
            .map(String::as_str)
        {
            Some(value) if value.eq_ignore_ascii_case("follower") => {
                tidb_executor::ReplicaReadType::Follower
            }
            Some(value) if value.eq_ignore_ascii_case("leader-and-follower") => {
                tidb_executor::ReplicaReadType::Mixed
            }
            Some(value) if value.eq_ignore_ascii_case("closest-replicas") => {
                tidb_executor::ReplicaReadType::Closest
            }
            Some(value) if value.eq_ignore_ascii_case("closest-adaptive") => {
                tidb_executor::ReplicaReadType::ClosestAdaptive
            }
            Some(value) if value.eq_ignore_ascii_case("learner") => {
                tidb_executor::ReplicaReadType::Learner
            }
            Some(value) if value.eq_ignore_ascii_case("prefer-leader") => {
                tidb_executor::ReplicaReadType::PreferLeader
            }
            _ => tidb_executor::ReplicaReadType::Leader,
        }
    }

    /// Go `SessionVars.IsAutocommit`, backed by its typed server-status bit.
    #[must_use]
    pub const fn is_autocommit(&self) -> bool {
        self.autocommit
    }

    /// Go `SessionVars.SQLMode`, parsed once when its sysvar changes.
    #[must_use]
    pub const fn sql_mode(&self) -> tidb_mysql::SqlMode {
        self.sql_mode
    }

    /// Go `SessionVars.MaxAllowedPacket`, parsed by the sysvar hook when the
    /// session copy changes rather than by each consumer.
    #[must_use]
    pub const fn max_allowed_packet(&self) -> u64 {
        self.max_allowed_packet
    }

    /// Go `SessionVars.GetMaxKeysRead`: `tidb_max_keys_read` limits index
    /// lookup work only inside a SELECT. DML and all non-SELECT statement
    /// contexts observe the zero (unlimited) sentinel even when the session
    /// has configured a positive value.
    #[must_use]
    pub const fn max_keys_read(&self, in_select_stmt: bool) -> u64 {
        if in_select_stmt {
            self.max_keys_read
        } else {
            0
        }
    }

    /// Go `SessionVars.MaxExecutionTime`, in milliseconds. A zero value
    /// preserves TiDB's unlimited-deadline sentinel.
    #[must_use]
    pub const fn max_execution_time(&self) -> u64 {
        self.max_execution_time
    }

    /// Go `SessionVars.TimeZone`, resolved from `SET time_zone` and retained
    /// until the next mutation or session-image reseed.
    #[must_use]
    pub fn session_time_zone(&self) -> tidb_executor::SessionTimeZone {
        self.time_zone.clone()
    }

    /// Go `SessionVars.SelectLimit`, where `u64::MAX` means unlimited.
    #[must_use]
    pub const fn select_limit(&self) -> u64 {
        self.select_limit
    }

    /// Go `SessionVars.MultiStatementMode`: OFF=0 refuses a multi-statement
    /// COM_QUERY without the client capability, ON=1 admits it, and WARN=2
    /// admits it while deferring warning 8130 to the final statement.
    #[must_use]
    pub const fn multi_statement_mode(&self) -> u8 {
        self.multi_statement_mode
    }

    /// Go `SessionVars.EnablePreparedPlanCache`, updated when its normalized
    /// ON/OFF sysvar changes rather than looked up by each execution.
    #[must_use]
    pub const fn prepared_plan_cache_enabled(&self) -> bool {
        self.enable_prepared_plan_cache
    }

    /// Go `SessionVars.EnableSharedLockUpgrade`, updated when its normalized
    /// ON/OFF sysvar changes and consumed by the transaction lock context.
    #[must_use]
    pub const fn shared_lock_upgrade_enabled(&self) -> bool {
        self.enable_shared_lock_upgrade
    }

    /// Go `SessionVars.EnableWindowFunction`, updated by the normalized
    /// `tidb_enable_window_function` bool sysvar.
    #[must_use]
    pub const fn window_function_enabled(&self) -> bool {
        self.enable_window_function
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalJoin`.
    #[must_use]
    pub const fn ti_flash_max_bytes_before_ext_join(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_join
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalGroupBy`.
    #[must_use]
    pub const fn ti_flash_max_bytes_before_ext_agg(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_agg
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalSort`.
    #[must_use]
    pub const fn ti_flash_max_bytes_before_ext_sort(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_sort
    }

    /// Go `SessionVars.TiFlashMemQuotaQueryPerNode`.
    #[must_use]
    pub const fn ti_flash_mem_quota_query_per_node(&self) -> i64 {
        self.ti_flash_mem_quota_query_per_node
    }

    /// Go `SessionVars.TiFlashQuerySpillRatio`.
    #[must_use]
    pub const fn ti_flash_query_spill_ratio(&self) -> f64 {
        self.ti_flash_query_spill_ratio
    }

    /// Go `SessionVars.PessimisticTransactionFairLocking`.
    #[must_use]
    pub const fn pessimistic_transaction_fair_locking_enabled(&self) -> bool {
        self.pessimistic_transaction_fair_locking
    }

    /// Go `SessionVars.BulkDMLEnabled`, set by `tidb_dml_type`.
    #[must_use]
    pub const fn bulk_dml_enabled(&self) -> bool {
        self.bulk_dml_enabled
    }

    /// Go `SessionVars.GetReplicaRead`, the typed KV replica-read mode.
    #[must_use]
    pub const fn replica_read(&self) -> tidb_executor::ReplicaReadType {
        self.replica_read
    }

    /// Updates ONE registry-indexed slot of the session image after the
    /// authoritative map changed. Statement-scoped save/restore pairs call
    /// this a handful of times each, so a whole-image rebuild here would run
    /// per statement -- the exact cost the image exists to avoid.
    ///
    /// `None` restores the registry default (the map no longer holds `name`).
    fn note_system_change(&mut self, name: &str) {
        let resolved = match self.systems.get(name) {
            Some(value) => Some(Arc::from(value.as_str())),
            None => None,
        };
        if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
            if let Some(slot) = self.session_resolved.values.get_mut(index) {
                *slot = resolved;
            }
        }
    }

    /// The session-tier twin of [`GlobalSysvars::refresh_resolved`]: one flat
    /// registry-indexed table of the current `systems` overrides.
    fn build_session_image(systems: &HashMap<String, String>) -> ResolvedGlobals {
        let mut slots: Vec<Option<Arc<str>>> =
            std::vec::Vec::with_capacity(crate::sysvar::SYS_VARS.len());
        slots.resize_with(crate::sysvar::SYS_VARS.len(), || None);
        for (name, value) in systems {
            if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
                slots[index] = Some(Arc::from(value.as_str()));
            }
        }
        ResolvedGlobals {
            values: slots.into(),
            ..ResolvedGlobals::default()
        }
    }

    /// Reads a system variable the way `SELECT @@name` does.
    ///
    /// A variable with SESSION scope answers from this session's own copy, and
    /// one with GLOBAL scope only answers from the shared table -- Go's
    /// `GetSessionOrGlobalSystemVar` falls through to `GetGlobalSystemVar`
    /// exactly when `sv.HasSessionScope()` is false. Without that fall-through
    /// a global-only variable would report the registry default forever, no
    /// matter what `SET GLOBAL` wrote (captured: after
    /// `set @@global.tidb_mem_oom_action='LOG'`, `select @@tidb_mem_oom_action`
    /// reports `LOG`).
    /// The mutation stamp for [`SessionVars`]-derived caches; see the field.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Reads a system variable without cloning its bytes when the session owns
    /// the value or the registry supplies a static default. This is Rust's
    /// equivalent of Go copying a string header out of `SessionVars.systems`.
    pub fn system_value(&self, name: &str) -> Result<Cow<'_, str>, VarError> {
        let Some(index) = crate::sysvar::sys_var_index_lookup(name) else {
            return Err(VarError::UnknownSystemVariable(name.to_ascii_lowercase()));
        };
        let def = &crate::sysvar::SYS_VARS[index];
        // An INSTANCE-scoped variable has no session copy either, and its
        // node-wide value is the only one there is: without this arm a
        // `SET GLOBAL tidb_general_log = 1` would store a value that
        // `SELECT @@tidb_general_log` never consults. A NONE-scope variable
        // (`port`, `socket`) reads the same node tier, which is where the
        // startup `set_global_vars` push (Go `variable.SetSysVar`) lives.
        if !def.has_session_scope() {
            return self.globals.get_by_registry_index(index).map(Cow::Owned);
        }
        if let Some(value) = self.session_resolved.values.get(index) {
            if let Some(value) = value.as_ref() {
                return Ok(Cow::Borrowed(value.as_ref()));
            }
            if self.session_resolved.values.len() == crate::sysvar::SYS_VARS.len() {
                // A full-length image is current by construction -- every
                // `systems` mutation republishes it before returning.
                return Ok(crate::sysvar::effective_default_value(def));
            }
        }
        let lowered = crate::sysvar::lowered_if_needed(name);
        self.systems.get(lowered.as_ref()).map_or_else(
            || Ok(crate::sysvar::effective_default_value(def)),
            |value| Ok(Cow::Borrowed(value.as_str())),
        )
    }

    pub fn get_system(&self, name: &str) -> Result<String, VarError> {
        self.system_value(name).map(Cow::into_owned)
    }

    /// A snapshot of the session overrides `name` (and its alias) currently
    /// hold, for a statement-scoped write to put back afterwards.
    ///
    /// `None` for a name with no override records the ABSENCE, so restoring it
    /// leaves the variable tracking the registry default rather than pinning
    /// it to the default's text.
    #[must_use]
    pub fn snapshot_system(&self, name: &str) -> Vec<(String, Option<String>)> {
        let key = name.to_ascii_lowercase();
        let mut keys = vec![key.clone()];
        if let Some(other) = alias_of(&key) {
            keys.push(other.to_owned());
        }
        keys.into_iter()
            .map(|key| {
                let previous = self.systems.get(&key).cloned();
                (key, previous)
            })
            .collect()
    }

    /// Puts back what [`Self::snapshot_system`] recorded.
    pub fn restore_system(&mut self, snapshot: Vec<(String, Option<String>)>) {
        if snapshot.is_empty() {
            return;
        }
        let mut restores_sql_mode = false;
        let mut restores_max_allowed_packet = false;
        let mut restores_max_keys_read = false;
        let mut restores_max_execution_time = false;
        let mut restores_time_zone = false;
        let mut restores_select_limit = false;
        let mut restores_multi_statement_mode = false;
        let mut restores_prepared_plan_cache = false;
        let mut restores_shared_lock_upgrade = false;
        let mut restores_window_function = false;
        let mut restores_ti_flash_max_bytes_before_ext_join = false;
        let mut restores_ti_flash_max_bytes_before_ext_agg = false;
        let mut restores_ti_flash_max_bytes_before_ext_sort = false;
        let mut restores_ti_flash_mem_quota_query_per_node = false;
        let mut restores_ti_flash_query_spill_ratio = false;
        let mut restores_pessimistic_transaction_fair_locking = false;
        let mut restores_bulk_dml_enabled = false;
        let mut restores_replica_read = false;
        for (key, previous) in snapshot {
            restores_sql_mode |= key == "sql_mode";
            restores_max_allowed_packet |= key == "max_allowed_packet";
            restores_max_keys_read |= key == "tidb_max_keys_read";
            restores_max_execution_time |= key == "max_execution_time";
            restores_time_zone |= key == "time_zone";
            restores_select_limit |= key == "sql_select_limit";
            restores_multi_statement_mode |= key == "tidb_multi_statement_mode";
            restores_prepared_plan_cache |=
                key == tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE;
            restores_shared_lock_upgrade |=
                key == tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE;
            restores_window_function |=
                key == tidb_vardef::tidb_vars::TIDB_ENABLE_WINDOW_FUNCTION;
            restores_ti_flash_max_bytes_before_ext_join |=
                key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN;
            restores_ti_flash_max_bytes_before_ext_agg |=
                key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY;
            restores_ti_flash_max_bytes_before_ext_sort |=
                key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT;
            restores_ti_flash_mem_quota_query_per_node |=
                key == tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE;
            restores_ti_flash_query_spill_ratio |=
                key == tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO;
            restores_pessimistic_transaction_fair_locking |=
                key == tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING;
            restores_bulk_dml_enabled |= key == tidb_vardef::tidb_vars::TIDB_DML_TYPE;
            restores_replica_read |= key == tidb_vardef::tidb_vars::TIDB_REPLICA_READ;
            match previous {
                Some(value) => {
                    self.session_resolved
                        .note(key.as_str(), Some(value.as_str()));
                    self.systems.insert(key, value);
                }
                None => {
                    self.session_resolved.note(key.as_str(), None);
                    self.systems.remove(&key);
                }
            };
        }
        self.generation += 1;
        self.autocommit = Self::autocommit_from_systems(&self.systems);
        if restores_sql_mode {
            self.sql_mode = Self::sql_mode_from_systems(&self.systems)
                .expect("a saved SQL mode was validated before it was stored");
        }
        if restores_max_allowed_packet {
            self.max_allowed_packet = Self::max_allowed_packet_from_systems(&self.systems)
                .expect("a saved max_allowed_packet was validated before it was stored");
        }
        if restores_max_keys_read {
            self.max_keys_read = Self::max_keys_read_from_systems(&self.systems);
        }
        if restores_max_execution_time {
            self.max_execution_time = Self::max_execution_time_from_systems(&self.systems);
        }
        if restores_time_zone {
            self.time_zone = Self::time_zone_from_systems(&self.systems);
        }
        if restores_select_limit {
            self.select_limit = Self::select_limit_from_systems(&self.systems);
        }
        if restores_multi_statement_mode {
            self.multi_statement_mode = Self::multi_statement_mode_from_systems(&self.systems);
        }
        if restores_prepared_plan_cache {
            self.enable_prepared_plan_cache = Self::prepared_plan_cache_from_systems(&self.systems);
        }
        if restores_shared_lock_upgrade {
            self.enable_shared_lock_upgrade = Self::shared_lock_upgrade_from_systems(&self.systems);
        }
        if restores_window_function {
            self.enable_window_function = Self::enable_window_function_from_systems(&self.systems);
        }
        if restores_ti_flash_max_bytes_before_ext_join {
            self.ti_flash_max_bytes_before_ext_join =
                Self::ti_flash_max_bytes_before_ext_join_from_systems(&self.systems);
        }
        if restores_ti_flash_max_bytes_before_ext_agg {
            self.ti_flash_max_bytes_before_ext_agg =
                Self::ti_flash_max_bytes_before_ext_agg_from_systems(&self.systems);
        }
        if restores_ti_flash_max_bytes_before_ext_sort {
            self.ti_flash_max_bytes_before_ext_sort =
                Self::ti_flash_max_bytes_before_ext_sort_from_systems(&self.systems);
        }
        if restores_ti_flash_mem_quota_query_per_node {
            self.ti_flash_mem_quota_query_per_node =
                Self::ti_flash_mem_quota_query_per_node_from_systems(&self.systems);
        }
        if restores_ti_flash_query_spill_ratio {
            self.ti_flash_query_spill_ratio =
                Self::ti_flash_query_spill_ratio_from_systems(&self.systems);
        }
        if restores_pessimistic_transaction_fair_locking {
            self.pessimistic_transaction_fair_locking =
                Self::pessimistic_transaction_fair_locking_from_systems(&self.systems);
        }
        if restores_bulk_dml_enabled {
            self.bulk_dml_enabled = Self::bulk_dml_enabled_from_systems(&self.systems);
        }
        if restores_replica_read {
            self.replica_read = Self::replica_read_from_systems(&self.systems);
        }
        self.refresh_optimizer_fix_control();
    }

    /// Reads `@@global.name`: always the shared table's live value, never
    /// this session's own copy. Go's `ErrIncorrectGlobalLocalVar` (1238) when
    /// the variable has no GLOBAL scope at all to read.
    pub fn get_global(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        // Go's read path for `@@global.x` does not run `validateScope`; an
        // instance-scoped variable answers `SELECT @@global.max_connections`,
        // which some drivers ask for at connect.
        if !def.has_global_scope() && !def.has_instance_scope() {
            // A NONE-scope variable (`port`, `socket`) has exactly ONE
            // value, the node's own, and Go answers it for `@@global.x` and
            // `SHOW GLOBAL VARIABLES` alike (`GetScopeNoneSystemVar`). Only
            // a SESSION-only variable has no global copy to read (Go's
            // ErrIncorrectGlobalLocalVar).
            if def.has_session_scope() {
                return Err(VarError::NoGlobalCopy(name.to_ascii_lowercase()));
            }
            return self.globals.get(name);
        }
        self.globals.get(name)
    }

    pub(crate) fn global_sysvar_accessor(&self) -> Arc<dyn tidb_executor::GlobalSysvarAccessor> {
        self.globals.clone()
    }

    /// Reads Go's two process-wide typed statement-memory settings without
    /// converting their GLOBAL sysvar text on every statement.
    pub(crate) fn statement_memory_policy(&self) -> (tidb_executor::OomAction, bool) {
        self.globals.statement_memory_policy()
    }

    /// Sets a session system variable, validating the value as Go's
    /// `ValidateFromType` does: the stored value is the normalized one, and
    /// an out-of-range value is clamped exactly as Go clamps it.
    ///
    /// A read-only variable is Go's `ErrIncorrectGlobalLocalVar`. A
    /// GLOBAL-only variable is Go's `ErrGlobalVariable` (1229): `SET
    /// SESSION`/plain `SET` cannot touch it, only `SET GLOBAL` can.
    ///
    /// Answers whether the value was clamped: Go's clamping checks
    /// (`checkUInt64SystemVar` and friends, plus the per-variable `Validation`
    /// closures modelled in [`sysvar`]) do not fail the statement, they append
    /// `ErrTruncatedWrongValue` (1292) to it, so the caller with the statement
    /// context in hand is the one that can report it.
    pub fn set_system(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_session_scope() {
            return Err(VarError::GlobalOnlyVariable(name.to_ascii_lowercase()));
        }
        let validated = def
            .validate_in_scope(&value, SCOPE_SESSION)
            .map_err(|error| validation_var_error(name, &value, error))?;
        let key = name.to_ascii_lowercase();
        let parsed_fix_control = if key == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL {
            Some(
                OptimizerFixControl::parse(&validated.value)
                    .map_err(|error| VarError::ValidationRefused(error.to_string()))?
                    .0,
            )
        } else {
            None
        };
        let parsed_sql_mode = if key == "sql_mode" {
            Some(
                tidb_mysql::get_sql_mode(&validated.value)
                    .map_err(|error| VarError::ValidationRefused(error.to_string()))?,
            )
        } else {
            None
        };
        if key == tidb_vardef::tidb_vars::TIDB_DML_TYPE
            && !validated.value.eq_ignore_ascii_case("standard")
            && !validated.value.eq_ignore_ascii_case("bulk")
        {
            return Err(VarError::ValidationRefused(format!(
                "unsupport DML type: {}",
                validated.value
            )));
        }
        // Go's `collation_server.SetSession` hook mirrors the selected
        // collation's owning charset into `character_set_server`. Keep both
        // names in the session image so a later `@@character_set_server` read
        // observes the same side effect as the source SessionVars map.
        let collation_server_charset = (key == "collation_server")
            .then(|| tidb_datatype::get_collation_by_name(&validated.value).ok())
            .flatten()
            .map(|collation| collation.charset_name);
        // Go `SetSessionFromHook`: the alias takes the SAME stored value, with
        // its own validation skipped -- `tx_isolation` and
        // `transaction_isolation` are one value under two spellings.
        if let Some(other) = alias_of(&key) {
            self.systems
                .insert(other.to_owned(), validated.value.clone());
        }
        self.systems.insert(key.clone(), validated.value.clone());
        self.note_system_change(&key);
        if let Some(other) = alias_of(&key) {
            self.note_system_change(other);
        }
        if let Some(charset) = collation_server_charset {
            self.systems
                .insert("character_set_server".to_owned(), charset);
            self.note_system_change("character_set_server");
        }
        if key == "autocommit" {
            self.autocommit = validated.value == "ON";
        }
        if let Some(parsed) = parsed_sql_mode {
            self.sql_mode = parsed;
        }
        if key == "max_allowed_packet" {
            self.max_allowed_packet = validated
                .value
                .parse::<u64>()
                .expect("max_allowed_packet validation stores unsigned decimal bytes");
        }
        if key == "tidb_max_keys_read" {
            self.max_keys_read = validated
                .value
                .parse::<u64>()
                .expect("tidb_max_keys_read validation stores unsigned decimal keys");
        }
        if key == "max_execution_time" {
            self.max_execution_time = validated
                .value
                .parse::<u64>()
                .expect("max_execution_time validation stores unsigned decimal milliseconds");
        }
        if key == "time_zone" {
            self.time_zone = resolve_session_time_zone_value(&validated.value);
        }
        if key == "sql_select_limit" {
            self.select_limit = validated
                .value
                .parse::<u64>()
                .expect("sql_select_limit validation stores unsigned decimal rows");
        }
        if key == "tidb_multi_statement_mode" {
            self.multi_statement_mode = Self::multi_statement_mode_from_systems(&self.systems);
        }
        if key == tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE {
            self.enable_prepared_plan_cache = validated.value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE {
            self.enable_shared_lock_upgrade = validated.value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_ENABLE_WINDOW_FUNCTION {
            self.enable_window_function = validated.value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN {
            self.ti_flash_max_bytes_before_ext_join = validated
                .value
                .parse::<i64>()
                .expect("TiFlash external join threshold validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY {
            self.ti_flash_max_bytes_before_ext_agg = validated
                .value
                .parse::<i64>()
                .expect("TiFlash external group-by threshold validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT {
            self.ti_flash_max_bytes_before_ext_sort = validated
                .value
                .parse::<i64>()
                .expect("TiFlash external sort threshold validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE {
            self.ti_flash_mem_quota_query_per_node = validated
                .value
                .parse::<i64>()
                .expect("TiFlash per-node quota validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO {
            self.ti_flash_query_spill_ratio = validated
                .value
                .parse::<f64>()
                .expect("TiFlash spill ratio validation stores a decimal fraction");
        }
        if key == tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING {
            self.pessimistic_transaction_fair_locking = validated.value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_DML_TYPE {
            self.bulk_dml_enabled = validated.value.eq_ignore_ascii_case("bulk");
        }
        if key == tidb_vardef::tidb_vars::TIDB_REPLICA_READ {
            self.replica_read = Self::replica_read_from_systems(&self.systems);
        }
        self.generation += 1;
        if let Some(parsed) = parsed_fix_control {
            self.optimizer_fix_control = parsed;
        }
        Ok(validated.truncated)
    }

    /// `SET GLOBAL name = value`: writes only the shared table, never this
    /// session's own `@@name`. Go's `ErrLocalVariable` (1228) when the
    /// variable is SESSION-only, so there is no global copy to set.
    pub fn set_global(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        self.globals
            .set_with_time_zone(name, value, &self.time_zone)
    }

    /// `SET INSTANCE name = value`: writes the node-local tier, never this
    /// session's own copy (an instance-scoped variable has none).
    pub fn set_instance(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        self.globals.set_instance(name, value)
    }

    /// Points this session at a different shared GLOBAL table for one
    /// statement, answering the one it was using -- the `GlobalSysvars` twin
    /// of [`crate::privilege::PrivilegeRegistry`]'s `swap_privileges`. A
    /// convergence node validates one `SET GLOBAL` against a scratch table
    /// read from the cluster before persisting it, and must be able to put
    /// the live table back unconditionally if the statement fails.
    pub fn swap_globals(&mut self, globals: GlobalSysvars) -> GlobalSysvars {
        let previous = std::mem::replace(&mut self.globals, Arc::new(globals));
        Arc::try_unwrap(previous).unwrap_or_else(|shared| (*shared).clone())
    }

    /// `SET GLOBAL name = DEFAULT`.
    pub fn reset_global(&mut self, name: &str) -> Result<(), VarError> {
        self.globals.reset(name)
    }

    /// `SET INSTANCE name = DEFAULT`.
    pub fn reset_instance(&mut self, name: &str) -> Result<(), VarError> {
        self.globals.reset_instance(name)
    }

    /// Go's `SET x = DEFAULT`.
    ///
    /// DEFAULT is not "forget the override". Go `SetExecutor.getVarValue`
    /// RESOLVES it to a string --
    /// `variable.GlobalSystemVariableInitialValue(sysVar.Name, sysVar.Value)`
    /// -- and then writes that string through the ordinary session-set path.
    /// The difference is the whole point of that call: four variables ship a
    /// registry value that a real install never runs with
    /// (`tidb_row_format_version` is `1` in the struct and `2` everywhere
    /// else), so clearing the override answers a value no TiDB reports.
    /// Captured on a v8.5.6 playground: `SET tidb_row_format_version =
    /// DEFAULT; SELECT @@tidb_row_format_version` reads `2` on a Go node.
    ///
    /// The environment is the stock one. `store_is_tikv` is a PROCESS fact in
    /// Go (`config.GetGlobalConfig().Store`) that this tier is not told, so
    /// `tidb_enable_async_commit`/`tidb_enable_1pc` resolve to their
    /// non-TiKV-store defaults here; every other override is unconditional.
    pub fn reset_system(&mut self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        let default = crate::sysvar::effective_default(def);
        let value = tidb_vardef::global_sysvar_initial::global_system_variable_initial_value(
            &name.to_ascii_lowercase(),
            &default,
            tidb_vardef::global_sysvar_initial::GlobalSysvarEnvironment {
                store_is_tikv: false,
                in_test: false,
                next_gen: false,
            },
        );
        self.set_system(name, value).map(|_truncated| ())
    }

    /// Go `SET NAMES <charset>`: sets the three client character-set
    /// variables together, plus the connection collation.
    pub fn set_names(&mut self, charset: &str, collation: Option<&str>) -> Result<(), VarError> {
        for name in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            self.set_system(name, charset.to_owned())?;
        }
        if let Some(collation) = collation {
            self.set_system("collation_connection", collation.to_owned())?;
        }
        Ok(())
    }

    /// The parsed `tidb_opt_fix_control` map used by planner consumers.
    #[must_use]
    pub fn optimizer_fix_control(&self) -> &OptimizerFixControl {
        &self.optimizer_fix_control
    }

    fn refresh_optimizer_fix_control(&mut self) {
        let raw = self
            .get_system(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
            .unwrap_or_default();
        self.optimizer_fix_control = OptimizerFixControl::parse(&raw)
            .map(|(parsed, _warnings)| parsed)
            .expect(
                "tidb_opt_fix_control session state comes only from validated writes or trusted global rows",
            );
    }
}

/// Go `MViewExecutionSessionVars`: the execution-scoped session variables
/// shared by MV build, refresh, and mvservice maintenance orchestration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MViewExecutionSessionVars {
    /// Go `MaintainMemQuota`.
    pub maintain_mem_quota: i64,
    /// Go `IsolationReadEngines`.
    pub isolation_read_engines: String,
    /// Go `TiFlashMaxThreads`.
    pub ti_flash_max_threads: i64,
    /// Go `TiFlashMaxBytesBeforeExtJoin`.
    pub ti_flash_max_bytes_before_ext_join: i64,
    /// Go `TiFlashMaxBytesBeforeExtAgg`.
    pub ti_flash_max_bytes_before_ext_agg: i64,
    /// Go `TiFlashMaxBytesBeforeExtSort`.
    pub ti_flash_max_bytes_before_ext_sort: i64,
    /// Go `TiFlashMemQuotaQueryPerNode`.
    pub ti_flash_mem_quota_query_per_node: i64,
    /// Go `TiFlashQuerySpillRatio`.
    pub ti_flash_query_spill_ratio: f64,
    /// Go `FineGrainedStreamCount`.
    pub fine_grained_stream_count: i64,
    /// Go `FineGrainedBatchSize`.
    pub fine_grained_batch_size: u64,
    /// Go `ImportThreads` (Go `int`).
    pub import_threads: i64,
    /// Go `ImportDiskQuota`.
    pub import_disk_quota: String,
}

/// Go `MViewExecutionSessionVarsApplyConfig`: describes how MV execution vars
/// should be applied onto a session. The caller chooses which mem-quota
/// sysvar should receive `maintain_mem_quota` and how apply / restore errors
/// should be reported. Go's closure fields become boxed callbacks; an error
/// callback receives the rendered error text.
#[derive(Default)]
pub struct MViewExecutionSessionVarsApplyConfig {
    /// Go `MaintainMemQuotaVarName`; empty selects
    /// `tidb_mview_maintain_mem_quota`.
    pub maintain_mem_quota_var_name: String,
    /// Go `MaintainIsolationReadEnginesVarName`; empty selects
    /// `tidb_mview_maintain_isolation_read_engines`.
    pub maintain_isolation_read_engines_var_name: String,
    /// Go `CaptureAppliedVars`.
    pub capture_applied_vars: Option<Box<dyn Fn(&SessionVars) -> MViewExecutionSessionVars>>,
    /// Go `BestEffort`.
    pub best_effort: bool,
    /// Go `InjectApplyError`: returning `Some` simulates the named
    /// variable's SET failing with that rendered error.
    pub inject_apply_error: Option<Box<dyn Fn(&str) -> Option<String>>>,
    /// Go `OnApplyError` (name, value, rendered error).
    pub on_apply_error: Option<Box<dyn Fn(&str, &str, &str)>>,
    /// Go `OnRestoreError` (name, origin value, current value, rendered
    /// error).
    pub on_restore_error: Option<Box<dyn Fn(&str, &str, &str, &str)>>,
}

/// Go's zero-value config.
impl MViewExecutionSessionVarsApplyConfig {
    /// Go `&MViewExecutionSessionVarsApplyConfig{}`: every knob at its
    /// zero value, defaulting the two maintained variable names.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

struct MViewExecutionSessionVarAssignment {
    name: String,
    value: String,
    failure_message: &'static str,
}

/// The undo handle Go returns as a `func()`. Go's closure captures the
/// session implicitly; Rust requires the caller to hand the session back, so
/// [`Self::restore`] takes `&mut SessionVars`. The handle borrows the
/// config's `OnRestoreError` callback, as Go's closure captures it.
pub struct MViewExecutionVarsRestore<'a> {
    origin: MViewExecutionSessionVars,
    applied: MViewExecutionSessionVars,
    maintain_mem_quota_var_name: String,
    maintain_isolation_read_engines_var_name: String,
    on_restore_error: Option<&'a dyn Fn(&str, &str, &str, &str)>,
    noop: bool,
}

impl MViewExecutionVarsRestore<'_> {
    /// Runs the captured restore assignments.
    pub fn restore(self, vars: &mut SessionVars) {
        if self.noop {
            return;
        }
        restore_m_view_execution_session_vars(
            vars,
            &self.origin,
            &self.applied,
            &self.maintain_mem_quota_var_name,
            &self.maintain_isolation_read_engines_var_name,
            self.on_restore_error.as_deref(),
        );
    }
}

/// Go `CaptureMViewExecutionSessionVars`: captures the user-facing MV
/// execution knobs that should be inherited by a later MV build/refresh job.
/// Go reads typed `SessionVars` fields maintained by `SetSession` hooks; the
/// Rust carrier reads the matching typed session fields rather than reparsing
/// the authoritative system-variable text at every capture.
#[must_use]
pub fn capture_m_view_execution_session_vars(vars: &SessionVars) -> MViewExecutionSessionVars {
    MViewExecutionSessionVars {
        maintain_mem_quota: system_i64(vars, tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA),
        isolation_read_engines: get_isolation_read_engines_string(vars),
        ti_flash_max_threads: system_i64(vars, tidb_vardef::tidb_vars::TIDB_MAX_TIFLASH_THREADS),
        ti_flash_max_bytes_before_ext_join: vars.ti_flash_max_bytes_before_ext_join(),
        ti_flash_max_bytes_before_ext_agg: vars.ti_flash_max_bytes_before_ext_agg(),
        ti_flash_max_bytes_before_ext_sort: vars.ti_flash_max_bytes_before_ext_sort(),
        ti_flash_mem_quota_query_per_node: vars.ti_flash_mem_quota_query_per_node(),
        ti_flash_query_spill_ratio: vars.ti_flash_query_spill_ratio(),
        fine_grained_stream_count: system_i64(
            vars,
            tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
        ),
        fine_grained_batch_size: system_u64(
            vars,
            tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
        ),
        import_threads: system_i64(
            vars,
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS,
        ),
        import_disk_quota: system_string(
            vars,
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA,
        ),
    }
}

/// Go `CaptureAppliedMViewExecutionSessionVars`: captures the
/// execution-related values that are currently in effect on a session after
/// MV execution vars have been applied to `tidb_mem_quota_query` and
/// `tidb_isolation_read_engines`.
#[must_use]
pub fn capture_applied_m_view_execution_session_vars(
    vars: &SessionVars,
) -> MViewExecutionSessionVars {
    MViewExecutionSessionVars {
        maintain_mem_quota: system_i64(vars, tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY),
        isolation_read_engines: get_isolation_read_engines_string(vars),
        ti_flash_max_threads: system_i64(vars, tidb_vardef::tidb_vars::TIDB_MAX_TIFLASH_THREADS),
        ti_flash_max_bytes_before_ext_join: vars.ti_flash_max_bytes_before_ext_join(),
        ti_flash_max_bytes_before_ext_agg: vars.ti_flash_max_bytes_before_ext_agg(),
        ti_flash_max_bytes_before_ext_sort: vars.ti_flash_max_bytes_before_ext_sort(),
        ti_flash_mem_quota_query_per_node: vars.ti_flash_mem_quota_query_per_node(),
        ti_flash_query_spill_ratio: vars.ti_flash_query_spill_ratio(),
        fine_grained_stream_count: system_i64(
            vars,
            tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
        ),
        fine_grained_batch_size: system_u64(
            vars,
            tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
        ),
        import_threads: system_i64(
            vars,
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS,
        ),
        import_disk_quota: system_string(
            vars,
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA,
        ),
    }
}

/// Go `GetIsolationReadEnginesString`: returns the current session string
/// value of `tidb_isolation_read_engines`, or its default when the session
/// has not loaded it yet.
#[must_use]
pub fn get_isolation_read_engines_string(vars: &SessionVars) -> String {
    if let Ok(value) = vars.get_system(tidb_vardef::tidb_vars::TIDB_ISOLATION_READ_ENGINES) {
        return value;
    }
    if let Some(def) =
        crate::sysvar::get_sys_var(tidb_vardef::tidb_vars::TIDB_ISOLATION_READ_ENGINES)
    {
        return def.value.to_owned();
    }
    String::new()
}

/// Go `MViewExecutionSessionVarsFromJob`: reconstructs the MV execution
/// variables from a job's system-variable envelope, falling back per field
/// to the default session's captured values. A nil job yields the captured
/// defaults untouched.
#[must_use]
pub fn m_view_execution_session_vars_from_job(
    job: Option<&Job>,
    default_vars: &SessionVars,
) -> MViewExecutionSessionVars {
    let mut target = capture_applied_m_view_execution_session_vars(default_vars);
    let Some(job) = job else {
        return target;
    };
    let read = |name: &str| -> Option<String> {
        job.get_system_var(name)
            .map(|value| value.to_utf8_lossy_go())
    };
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.maintain_mem_quota = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES) {
        target.isolation_read_engines = value;
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MAX_TIFLASH_THREADS) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.ti_flash_max_threads = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.ti_flash_max_bytes_before_ext_join = parsed;
        }
    }
    if let Some(value) =
        read(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY)
    {
        if let Ok(parsed) = value.parse::<i64>() {
            target.ti_flash_max_bytes_before_ext_agg = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.ti_flash_max_bytes_before_ext_sort = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.ti_flash_mem_quota_query_per_node = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO) {
        if let Ok(parsed) = value.parse::<f64>() {
            target.ti_flash_query_spill_ratio = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.fine_grained_stream_count = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE) {
        if let Ok(parsed) = value.parse::<u64>() {
            target.fine_grained_batch_size = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS) {
        if let Ok(parsed) = value.parse::<i64>() {
            target.import_threads = parsed;
        }
    }
    if let Some(value) = read(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA) {
        target.import_disk_quota = value;
    }
    target
}

/// Go `ApplyMViewExecutionSessionVarsWithConfig`.
pub fn apply_m_view_execution_session_vars_with_config<'a>(
    vars: &mut SessionVars,
    target: &MViewExecutionSessionVars,
    cfg: &'a MViewExecutionSessionVarsApplyConfig,
) -> Result<MViewExecutionVarsRestore<'a>, String> {
    let default_capture = capture_applied_m_view_execution_session_vars
        as fn(&SessionVars) -> MViewExecutionSessionVars;
    let capture_fn: &dyn Fn(&SessionVars) -> MViewExecutionSessionVars = cfg
        .capture_applied_vars
        .as_deref()
        .unwrap_or(&default_capture);
    let maintain_mem_quota_var_name = if cfg.maintain_mem_quota_var_name.is_empty() {
        tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA.to_owned()
    } else {
        cfg.maintain_mem_quota_var_name.clone()
    };
    let maintain_isolation_read_engines_var_name =
        if cfg.maintain_isolation_read_engines_var_name.is_empty() {
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES.to_owned()
        } else {
            cfg.maintain_isolation_read_engines_var_name.clone()
        };

    let origin = capture_fn(vars);
    if origin == *target {
        return Ok(MViewExecutionVarsRestore {
            origin,
            applied: target.clone(),
            maintain_mem_quota_var_name,
            maintain_isolation_read_engines_var_name,
            on_restore_error: None,
            noop: true,
        });
    }
    let assignments = build_m_view_execution_session_var_assignments(
        target,
        &maintain_mem_quota_var_name,
        &maintain_isolation_read_engines_var_name,
    );
    for assignment in &assignments {
        let injected = cfg
            .inject_apply_error
            .as_deref()
            .and_then(|inject| inject(&assignment.name));
        let outcome = match injected {
            Some(error) => Err(error),
            None => vars
                .set_system(&assignment.name, assignment.value.clone())
                .map(|_| String::new())
                .map_err(|error| render_var_error(&error)),
        };
        let error = match outcome {
            Ok(_) => continue,
            Err(error) => error,
        };
        if !cfg.best_effort {
            let current = capture_fn(vars);
            restore_m_view_execution_session_vars(
                vars,
                &origin,
                &current,
                &maintain_mem_quota_var_name,
                &maintain_isolation_read_engines_var_name,
                cfg.on_restore_error.as_deref(),
            );
            return Err(format!("{}: {}", assignment.failure_message, error));
        }
        if let Some(on_apply_error) = cfg.on_apply_error.as_deref() {
            on_apply_error(&assignment.name, &assignment.value, &error);
        }
    }

    let applied = capture_fn(vars);
    Ok(MViewExecutionVarsRestore {
        origin,
        applied,
        maintain_mem_quota_var_name,
        maintain_isolation_read_engines_var_name,
        on_restore_error: cfg.on_restore_error.as_deref(),
        noop: false,
    })
}

fn build_m_view_execution_session_var_assignments(
    target: &MViewExecutionSessionVars,
    maintain_mem_quota_var_name: &str,
    maintain_isolation_read_engines_var_name: &str,
) -> Vec<MViewExecutionSessionVarAssignment> {
    vec![
        MViewExecutionSessionVarAssignment {
            name: maintain_mem_quota_var_name.to_owned(),
            value: target.maintain_mem_quota.to_string(),
            failure_message: "mv execution: failed to apply maintain mem quota",
        },
        MViewExecutionSessionVarAssignment {
            name: maintain_isolation_read_engines_var_name.to_owned(),
            value: target.isolation_read_engines.clone(),
            failure_message: "mv execution: failed to apply tidb_isolation_read_engines",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIDB_MAX_TIFLASH_THREADS.to_owned(),
            value: target.ti_flash_max_threads.to_string(),
            failure_message: "mv execution: failed to apply tidb_max_tiflash_threads",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN.to_owned(),
            value: target.ti_flash_max_bytes_before_ext_join.to_string(),
            failure_message:
                "mv execution: failed to apply tidb_max_bytes_before_tiflash_external_join",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY
                .to_owned(),
            value: target.ti_flash_max_bytes_before_ext_agg.to_string(),
            failure_message:
                "mv execution: failed to apply tidb_max_bytes_before_tiflash_external_group_by",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT.to_owned(),
            value: target.ti_flash_max_bytes_before_ext_sort.to_string(),
            failure_message:
                "mv execution: failed to apply tidb_max_bytes_before_tiflash_external_sort",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE.to_owned(),
            value: target.ti_flash_mem_quota_query_per_node.to_string(),
            failure_message: "mv execution: failed to apply tiflash_mem_quota_query_per_node",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO.to_owned(),
            value: format_double(target.ti_flash_query_spill_ratio),
            failure_message: "mv execution: failed to apply tiflash_query_spill_ratio",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT.to_owned(),
            value: target.fine_grained_stream_count.to_string(),
            failure_message:
                "mv execution: failed to apply tiflash_fine_grained_shuffle_stream_count",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE.to_owned(),
            value: target.fine_grained_batch_size.to_string(),
            failure_message:
                "mv execution: failed to apply tiflash_fine_grained_shuffle_batch_size",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS.to_owned(),
            value: target.import_threads.to_string(),
            failure_message: "mv execution: failed to apply tidb_mview_maintain_import_threads",
        },
        MViewExecutionSessionVarAssignment {
            name: tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA.to_owned(),
            value: target.import_disk_quota.clone(),
            failure_message: "mv execution: failed to apply tidb_mview_maintain_import_disk_quota",
        },
    ]
}

fn restore_m_view_execution_session_vars(
    vars: &mut SessionVars,
    origin: &MViewExecutionSessionVars,
    current: &MViewExecutionSessionVars,
    maintain_mem_quota_var_name: &str,
    maintain_isolation_read_engines_var_name: &str,
    on_restore_error: Option<&dyn Fn(&str, &str, &str, &str)>,
) {
    let origin_assignments = build_m_view_execution_session_var_assignments(
        origin,
        maintain_mem_quota_var_name,
        maintain_isolation_read_engines_var_name,
    );
    let current_assignments = build_m_view_execution_session_var_assignments(
        current,
        maintain_mem_quota_var_name,
        maintain_isolation_read_engines_var_name,
    );
    for (index, assignment) in origin_assignments.iter().enumerate() {
        if let Err(error) = vars.set_system(&assignment.name, assignment.value.clone()) {
            if let Some(on_restore_error) = on_restore_error {
                on_restore_error(
                    &assignment.name,
                    &assignment.value,
                    &current_assignments[index].value,
                    &render_var_error(&error),
                );
            }
        }
    }
}

/// Go `AddMViewExecutionSessionVarsToJob`'s capture side: the live values of
/// the twelve MV-execution session variables as the (name, value) image the
/// DDL job envelope snapshots. Go's job writer reads the same typed fields
/// off `SessionVars`; the names are the canonical sysvar names Go stores in
/// the job's system-var list (the default maintained-variable names). The
/// session tier installs this image on every DDL statement context, so a
/// submitted MV job inherits the creator's settings instead of the
/// defaults.
#[must_use]
pub fn m_view_execution_session_vars_image(
    vars: &SessionVars,
) -> std::collections::BTreeMap<String, String> {
    let captured = capture_m_view_execution_session_vars(vars);
    build_m_view_execution_session_var_assignments(
        &captured,
        tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA,
        tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES,
    )
    .into_iter()
    .map(|assignment| (assignment.name, assignment.value))
    .collect()
}

fn system_i64(vars: &SessionVars, name: &str) -> i64 {
    vars.get_system(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn system_u64(vars: &SessionVars, name: &str) -> u64 {
    vars.get_system(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn system_f64(vars: &SessionVars, name: &str) -> f64 {
    vars.get_system(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0)
}

fn system_string(vars: &SessionVars, name: &str) -> String {
    vars.get_system(name).unwrap_or_default()
}

/// Renders a [`VarError`] with Go's system-variable message text for the
/// variants Go's `SetSystemVar` produces here; the callback payloads carry
/// the same text Go's `errors.Annotate` would.
fn render_var_error(error: &VarError) -> String {
    match error {
        VarError::UnknownSystemVariable(name) => format!("Unknown system variable '{name}'"),
        VarError::ReadOnlyVariable(name) => {
            format!("Variable '{name}' is a read only variable")
        }
        VarError::WrongValueForVar(name, value) => {
            format!("Wrong value for variable '{name}': '{value}'")
        }
        VarError::GlobalOnlyVariable(name) => {
            format!("Variable {name} is a global variable and should be set with SET GLOBAL")
        }
        VarError::SessionOnlyVariable(name) => {
            format!("Variable {name} is a session variable and can not be used with SET GLOBAL")
        }
        VarError::ValidationRefused(message) => message.clone(),
        other => format!("{other:?}"),
    }
}

/// Go `strconv.FormatFloat(value, 'f', -1, 64)`: the shortest decimal that
/// round-trips, without an exponent for the values these variables hold.
fn format_double(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{}", value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RestoreAnalyzeDefaults {
        buckets: u64,
        top_n: u64,
        stats_cache_mem_quota: i64,
    }

    impl Drop for RestoreAnalyzeDefaults {
        fn drop(&mut self) {
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS
                .store(self.buckets, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N
                .store(self.top_n, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::STATS_CACHE_MEM_QUOTA.store(
                self.stats_cache_mem_quota,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }

    #[test]
    fn tiflash_set_session_hooks_update_typed_state_and_mview_capture() {
        let mut vars = SessionVars::new();
        assert_eq!(
            vars.ti_flash_max_bytes_before_ext_join(),
            tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN
        );
        assert_eq!(
            vars.ti_flash_mem_quota_query_per_node(),
            tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE
        );
        assert_eq!(
            vars.ti_flash_query_spill_ratio(),
            tidb_vardef::defaults::DEF_TIFLASH_QUERY_SPILL_RATIO
        );

        let join_snapshot = vars
            .snapshot_system(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN);
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
            "10000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
            "20000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
            "30000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            "40000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO,
            "0.75".to_owned(),
        )
        .unwrap();

        assert_eq!(vars.ti_flash_max_bytes_before_ext_join(), 10_000);
        assert_eq!(vars.ti_flash_max_bytes_before_ext_agg(), 20_000);
        assert_eq!(vars.ti_flash_max_bytes_before_ext_sort(), 30_000);
        assert_eq!(vars.ti_flash_mem_quota_query_per_node(), 40_000);
        assert_eq!(vars.ti_flash_query_spill_ratio(), 0.75);

        let captured = capture_m_view_execution_session_vars(&vars);
        assert_eq!(captured.ti_flash_max_bytes_before_ext_join, 10_000);
        assert_eq!(captured.ti_flash_max_bytes_before_ext_agg, 20_000);
        assert_eq!(captured.ti_flash_max_bytes_before_ext_sort, 30_000);
        assert_eq!(captured.ti_flash_mem_quota_query_per_node, 40_000);
        assert_eq!(captured.ti_flash_query_spill_ratio, 0.75);

        vars.restore_system(join_snapshot);
        assert_eq!(
            vars.ti_flash_max_bytes_before_ext_join(),
            tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN
        );
    }

    #[test]
    fn tiflash_global_values_seed_typed_state_for_new_sessions() {
        let globals = GlobalSysvars::new();
        globals
            .set(
                tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO,
                "0.85".to_owned(),
            )
            .unwrap();
        globals
            .set(
                tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
                "10000".to_owned(),
            )
            .unwrap();

        let mut vars = SessionVars::new();
        vars.seed_from_globals(globals).unwrap();
        assert_eq!(vars.ti_flash_query_spill_ratio(), 0.85);
        assert_eq!(vars.ti_flash_mem_quota_query_per_node(), 10_000);
        assert_eq!(
            vars.get_system(tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO)
                .unwrap(),
            "0.85"
        );
    }

    #[test]
    fn pessimistic_fair_locking_uses_go_typed_session_hook() {
        let mut vars = SessionVars::new();
        assert!(!vars.pessimistic_transaction_fair_locking_enabled());

        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
            "OFF".to_owned(),
        )
        .unwrap();
        assert!(!vars.pessimistic_transaction_fair_locking_enabled());

        if tidb_config::kerneltype::is_next_gen() {
            let error = vars
                .set_system(
                    tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
                    "ON".to_owned(),
                )
                .expect_err("nextgen rejects fair-locking ON");
            let VarError::SqlError(error) = error else {
                panic!("nextgen fair-locking refusal should retain MySQL error 1235");
            };
            assert_eq!(error.code, 1235);
            assert!(!vars.pessimistic_transaction_fair_locking_enabled());
        } else {
            vars.set_system(
                tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
                "ON".to_owned(),
            )
            .unwrap();
            assert!(vars.pessimistic_transaction_fair_locking_enabled());
        }
    }

    #[test]
    fn dml_type_and_replica_read_use_go_typed_hooks() {
        let mut vars = SessionVars::new();
        assert!(!vars.bulk_dml_enabled());
        assert_eq!(vars.replica_read(), tidb_executor::ReplicaReadType::Leader);

        vars.set_system(tidb_vardef::tidb_vars::TIDB_DML_TYPE, "standard".to_owned())
            .unwrap();
        assert!(!vars.bulk_dml_enabled());

        if tidb_config::kerneltype::is_next_gen() {
            let error = vars
                .set_system(tidb_vardef::tidb_vars::TIDB_DML_TYPE, "bulk".to_owned())
                .expect_err("nextgen rejects bulk DML");
            let VarError::SqlError(error) = error else {
                panic!("nextgen bulk-DML refusal should retain MySQL error 1235");
            };
            assert_eq!(error.code, 1235);
            assert!(!vars.bulk_dml_enabled());
        } else {
            vars.set_system(tidb_vardef::tidb_vars::TIDB_DML_TYPE, "bulk".to_owned())
                .unwrap();
            assert!(vars.bulk_dml_enabled());
        }

        let modes = [
            ("follower", tidb_executor::ReplicaReadType::Follower),
            ("leader-and-follower", tidb_executor::ReplicaReadType::Mixed),
            ("closest-replicas", tidb_executor::ReplicaReadType::Closest),
            (
                "closest-adaptive",
                tidb_executor::ReplicaReadType::ClosestAdaptive,
            ),
            ("learner", tidb_executor::ReplicaReadType::Learner),
            (
                "prefer-leader",
                tidb_executor::ReplicaReadType::PreferLeader,
            ),
        ];
        for (name, expected) in modes {
            if tidb_config::kerneltype::is_next_gen() {
                let error = vars
                    .set_system(tidb_vardef::tidb_vars::TIDB_REPLICA_READ, name.to_owned())
                    .expect_err("nextgen rejects non-leader replica reads");
                let VarError::SqlError(error) = error else {
                    panic!("nextgen replica-read refusal should retain MySQL error 1235");
                };
                assert_eq!(error.code, 1235);
                assert_eq!(vars.replica_read(), tidb_executor::ReplicaReadType::Leader);
            } else {
                vars.set_system(tidb_vardef::tidb_vars::TIDB_REPLICA_READ, name.to_owned())
                    .unwrap();
                assert_eq!(vars.replica_read(), expected);
            }
        }
    }

    #[test]
    fn global_analyze_defaults_update_the_vardef_backing_values() {
        let _restore = RestoreAnalyzeDefaults {
            buckets: tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS
                .load(std::sync::atomic::Ordering::SeqCst),
            top_n: tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
            stats_cache_mem_quota: tidb_vardef::STATS_CACHE_MEM_QUOTA
                .load(std::sync::atomic::Ordering::SeqCst),
        };
        let globals = GlobalSysvars::new();

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_BUCKETS,
                "4".to_owned(),
            )
            .unwrap();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_TOP_N,
                "5".to_owned(),
            )
            .unwrap();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_STATS_CACHE_MEM_QUOTA,
                "6".to_owned(),
            )
            .unwrap();
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
            4
        );
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
            5
        );
        assert_eq!(
            tidb_vardef::STATS_CACHE_MEM_QUOTA.load(std::sync::atomic::Ordering::SeqCst),
            6
        );

        globals
            .reset(tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_BUCKETS)
            .unwrap();
        globals
            .reset(tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_TOP_N)
            .unwrap();
        globals
            .reset(tidb_vardef::tidb_vars::TIDB_STATS_CACHE_MEM_QUOTA)
            .unwrap();
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
            tidb_vardef::defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_BUCKETS as u64
        );
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
            tidb_vardef::defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_TOP_N as u64
        );
        assert_eq!(
            tidb_vardef::STATS_CACHE_MEM_QUOTA.load(std::sync::atomic::Ordering::SeqCst),
            tidb_vardef::defaults::DEF_TIDB_STATS_CACHE_MEM_QUOTA
        );
    }

    #[test]
    fn empty_statement_restore_preserves_the_session_generation() {
        let mut vars = SessionVars::new();
        let generation = vars.generation();

        vars.restore_system(Vec::new());

        assert_eq!(vars.generation(), generation);
    }

    #[test]
    fn session_autocommit_uses_go_typed_status() {
        let source = include_str!("txn.rs");
        let body = source
            .split_once("pub fn is_autocommit")
            .expect("session autocommit accessor")
            .1
            .split_once("/// Go's lazy transaction start")
            .expect("end of session autocommit accessor")
            .0;

        assert!(body.contains("self.vars.is_autocommit()"));
        assert!(!body.contains("get_system"));

        let mut vars = SessionVars::new();
        assert!(vars.is_autocommit());
        vars.set_system("autocommit", "OFF".to_owned()).unwrap();
        assert!(!vars.is_autocommit());
        let restore = vars.snapshot_system("autocommit");
        vars.set_system("autocommit", "ON".to_owned()).unwrap();
        assert!(vars.is_autocommit());
        vars.restore_system(restore);
        assert!(!vars.is_autocommit());

        let globals = GlobalSysvars::new();
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert!(!inherited.is_autocommit());
    }

    #[test]
    fn session_sql_mode_uses_go_typed_state() {
        let mut vars = SessionVars::new();
        assert!(vars.sql_mode().has_strict_mode());
        assert!(vars.sql_mode().has_only_full_group_by());

        let restore = vars.snapshot_system("sql_mode");
        vars.set_system("sql_mode", "strict_trans_tabLES  ".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("sql_mode").unwrap(), "STRICT_TRANS_TABLES");
        assert!(vars.sql_mode().has_strict_mode());
        let error = vars
            .set_system("sql_mode", "strict_trans_tabLES,nonsense_option".to_owned())
            .unwrap_err();
        let VarError::SqlError(error) = error else {
            panic!("expected catalogued SQL error");
        };
        assert_eq!(error.code, 1231);
        assert_eq!(error.state, "42000");
        assert_eq!(
            error.message,
            "Variable 'sql_mode' can't be set to the value of 'NONSENSE_OPTION'"
        );
        assert!(vars.sql_mode().has_strict_mode());

        vars.set_system("sql_mode", "ANSI".to_owned()).unwrap();
        assert!(vars.sql_mode().has_ansi_quotes_mode());
        assert!(vars.sql_mode().has_pipes_as_concat_mode());
        assert!(!vars.sql_mode().has_strict_mode());
        vars.restore_system(restore);
        assert!(vars.sql_mode().has_strict_mode());
        assert!(!vars.sql_mode().has_ansi_quotes_mode());

        let globals = GlobalSysvars::new();
        globals
            .set("sql_mode", "NO_UNSIGNED_SUBTRACTION".to_owned())
            .unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert!(inherited.sql_mode().has_no_unsigned_subtraction_mode());
        assert!(!inherited.sql_mode().has_strict_mode());
    }

    /// Go `TestTiDBMaxKeysRead` + `TestGetMaxKeysRead`: validation clips a
    /// negative value to zero, the session hook stores the positive value in
    /// typed state, and the accessor returns that state only for SELECTs.
    #[test]
    fn max_keys_read_uses_go_select_gate_and_typed_state() {
        let definition = get_sys_var("tidb_max_keys_read").unwrap();
        assert_eq!(definition.validate("-1").unwrap().value, "0");
        assert_eq!(definition.validate("0").unwrap().value, "0");
        assert_eq!(definition.validate("1000").unwrap().value, "1000");

        let mut vars = SessionVars::new();
        assert_eq!(vars.max_keys_read(false), 0);
        assert_eq!(vars.max_keys_read(true), 0);

        vars.set_system("tidb_max_keys_read", "500".to_owned())
            .unwrap();
        assert_eq!(vars.max_keys_read(false), 0);
        assert_eq!(vars.max_keys_read(true), 500);

        let restore = vars.snapshot_system("tidb_max_keys_read");
        vars.set_system("tidb_max_keys_read", "100".to_owned())
            .unwrap();
        vars.restore_system(restore);
        assert_eq!(vars.max_keys_read(true), 500);

        let globals = GlobalSysvars::new();
        globals.set("tidb_max_keys_read", "100".to_owned()).unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert_eq!(inherited.max_keys_read(true), 100);
        assert_eq!(inherited.max_keys_read(false), 0);
    }

    /// Go `TestMaxExecutionTime`: the unsigned value clamps a negative input,
    /// the session hook publishes the millisecond deadline into typed state,
    /// and statement-scoped restore/global inheritance keep that state in
    /// sync with the authoritative variable image.
    #[test]
    fn max_execution_time_uses_go_typed_state() {
        let definition = get_sys_var("max_execution_time").unwrap();
        assert_eq!(definition.validate("-10").unwrap().value, "0");
        assert_eq!(definition.validate("99999").unwrap().value, "99999");

        let mut vars = SessionVars::new();
        assert_eq!(vars.max_execution_time(), 0);
        vars.set_system("max_execution_time", "99999".to_owned())
            .unwrap();
        assert_eq!(vars.max_execution_time(), 99999);

        let restore = vars.snapshot_system("max_execution_time");
        vars.set_system("max_execution_time", "100".to_owned())
            .unwrap();
        assert_eq!(vars.max_execution_time(), 100);
        vars.restore_system(restore);
        assert_eq!(vars.max_execution_time(), 99999);

        let globals = GlobalSysvars::new();
        globals.set("max_execution_time", "250".to_owned()).unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert_eq!(inherited.max_execution_time(), 250);
    }

    /// Go `TestTiDBMultiStatementMode`: enum spellings normalize to the
    /// SessionVars integer mode, statement overlays can restore it, and a
    /// newly connected session inherits the GLOBAL value.
    #[test]
    fn multi_statement_mode_uses_go_typed_state() {
        let definition = get_sys_var("tidb_multi_statement_mode").unwrap();
        assert_eq!(definition.validate("on").unwrap().value, "ON");
        assert_eq!(definition.validate("0").unwrap().value, "OFF");
        assert_eq!(definition.validate("Warn").unwrap().value, "WARN");

        let mut vars = SessionVars::new();
        assert_eq!(vars.multi_statement_mode(), 0);
        vars.set_system("tidb_multi_statement_mode", "ON".to_owned())
            .unwrap();
        assert_eq!(vars.multi_statement_mode(), 1);

        let restore = vars.snapshot_system("tidb_multi_statement_mode");
        vars.set_system("tidb_multi_statement_mode", "WARN".to_owned())
            .unwrap();
        assert_eq!(vars.multi_statement_mode(), 2);
        vars.restore_system(restore);
        assert_eq!(vars.multi_statement_mode(), 1);

        let globals = GlobalSysvars::new();
        globals
            .set("tidb_multi_statement_mode", "WARN".to_owned())
            .unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert_eq!(inherited.multi_statement_mode(), 2);
    }

    /// Go `TestSQLSelectLimit`: the unsigned limit clips negatives to zero,
    /// stores the normalized value in `SessionVars.SelectLimit`, and restores
    /// the unlimited MaxUint64 default through the ordinary session image.
    #[test]
    fn sql_select_limit_uses_go_typed_state() {
        let definition = get_sys_var("sql_select_limit").unwrap();
        assert_eq!(definition.validate("-10").unwrap().value, "0");
        assert_eq!(definition.validate("9999").unwrap().value, "9999");

        let mut vars = SessionVars::new();
        assert_eq!(vars.select_limit(), u64::MAX);
        vars.set_system("sql_select_limit", "9999".to_owned())
            .unwrap();
        assert_eq!(vars.select_limit(), 9999);

        let restore = vars.snapshot_system("sql_select_limit");
        vars.set_system("sql_select_limit", "0".to_owned()).unwrap();
        assert_eq!(vars.select_limit(), 0);
        vars.restore_system(restore);
        assert_eq!(vars.select_limit(), 9999);

        let globals = GlobalSysvars::new();
        globals.set("sql_select_limit", "2".to_owned()).unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert_eq!(inherited.select_limit(), 2);
    }

    /// Go `TestTimeZone`: validated names and fixed offsets are resolved by
    /// the session hook, retained for statement contexts, and restored or
    /// inherited with the session image.
    #[test]
    fn time_zone_uses_go_typed_state() {
        let definition = get_sys_var("time_zone").unwrap();
        for value in ["America/Edmonton", "+10:00", "UTC", "+00:00"] {
            assert_eq!(definition.validate(value).unwrap().value, value);
        }

        let mut vars = SessionVars::new();
        vars.set_system("time_zone", "+10:00".to_owned()).unwrap();
        assert_eq!(
            vars.session_time_zone(),
            tidb_executor::SessionTimeZone::Fixed {
                name: "+10:00".to_owned(),
                offset_secs: 10 * 60 * 60,
            }
        );

        let restore = vars.snapshot_system("time_zone");
        vars.set_system("time_zone", "UTC".to_owned()).unwrap();
        assert_eq!(
            vars.session_time_zone(),
            tidb_executor::SessionTimeZone::Named(chrono_tz::Tz::UTC)
        );
        vars.restore_system(restore);
        assert_eq!(
            vars.session_time_zone(),
            tidb_executor::SessionTimeZone::Fixed {
                name: "+10:00".to_owned(),
                offset_secs: 10 * 60 * 60,
            }
        );

        let globals = GlobalSysvars::new();
        globals.set("time_zone", "UTC".to_owned()).unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert_eq!(
            inherited.session_time_zone(),
            tidb_executor::SessionTimeZone::Named(chrono_tz::Tz::UTC)
        );
    }

    /// Transcreated from Go `TestSetJobScheduleWindow`: a short TTL schedule
    /// time is interpreted in the issuing session's location, while an
    /// already-expanded value keeps its explicit numeric offset.
    #[test]
    fn ttl_schedule_window_global_write_uses_session_time_zone() {
        let mut vars = SessionVars::new();
        vars.set_system("time_zone", "UTC".to_owned()).unwrap();
        vars.set_global(
            "tidb_ttl_job_schedule_window_start_time",
            "16:11".to_owned(),
        )
        .unwrap();
        assert_eq!(
            vars.get_global("tidb_ttl_job_schedule_window_start_time")
                .unwrap(),
            "16:11 +0000"
        );

        vars.set_system("time_zone", "Asia/Shanghai".to_owned())
            .unwrap();
        vars.set_global(
            "tidb_ttl_job_schedule_window_start_time",
            "16:11".to_owned(),
        )
        .unwrap();
        assert_eq!(
            vars.get_global("tidb_ttl_job_schedule_window_start_time")
                .unwrap(),
            "16:11 +0800"
        );
        vars.set_global(
            "tidb_ttl_job_schedule_window_start_time",
            "16:11 +0000".to_owned(),
        )
        .unwrap();
        assert_eq!(
            vars.get_global("tidb_ttl_job_schedule_window_start_time")
                .unwrap(),
            "16:11 +0000"
        );
    }

    #[test]
    fn protocol_hot_path_reads_retained_session_state() {
        let session = include_str!("lib.rs");
        let wait_timeout = session
            .split_once("pub fn wait_timeout(&self)")
            .expect("session wait-timeout accessor")
            .1
            .split_once("/// A session sharing")
            .expect("end of session wait-timeout accessor")
            .0;
        assert!(!wait_timeout.contains("get_system"));

        let warnings = include_str!("warnings.rs");
        let charsets = warnings
            .split_once("pub fn result_charset(&self)")
            .expect("session result-charset accessor")
            .1
            .split_once("/// The warning count the OK/EOF packet carries")
            .expect("end of session charset accessors")
            .0;
        assert!(!charsets.contains("get_system"));
        assert!(warnings.contains("pub fn result_charset(&self) -> Cow<'_, str>"));
        assert!(warnings.contains("pub fn input_charset(&self) -> Cow<'_, str>"));

        let mut vars = SessionVars::new();
        assert_eq!(vars.max_allowed_packet(), 64 << 20);
        assert!(matches!(
            vars.system_value("character_set_results"),
            Ok(Cow::Borrowed("utf8mb4"))
        ));

        let max_restore = vars.snapshot_system("max_allowed_packet");
        vars.set_system("max_allowed_packet", "2048".to_owned())
            .unwrap();
        assert_eq!(vars.max_allowed_packet(), 2048);
        vars.restore_system(max_restore);
        assert_eq!(vars.max_allowed_packet(), 64 << 20);

        let globals = GlobalSysvars::new();
        globals
            .set("max_allowed_packet", "4096".to_owned())
            .unwrap();
        globals.set("wait_timeout", "17".to_owned()).unwrap();
        globals
            .set("character_set_results", "latin1".to_owned())
            .unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert_eq!(inherited.max_allowed_packet(), 4096);
        assert!(matches!(
            inherited.system_value("wait_timeout"),
            Ok(Cow::Borrowed("17"))
        ));
        assert!(matches!(
            inherited.system_value("character_set_results"),
            Ok(Cow::Borrowed("latin1"))
        ));
    }

    #[test]
    fn prepared_plan_cache_switch_uses_go_typed_state() {
        let admission = include_str!("prepared_plan_cache.rs");
        assert!(!admission.contains("get_system"));

        let execution = include_str!("prepared_ast.rs");
        assert_eq!(
            execution
                .matches("self.vars.prepared_plan_cache_enabled()")
                .count(),
            3
        );

        let mut vars = SessionVars::new();
        assert!(vars.prepared_plan_cache_enabled());
        let restore = vars.snapshot_system(tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE);
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE,
            "OFF".to_owned(),
        )
        .unwrap();
        assert!(!vars.prepared_plan_cache_enabled());
        vars.restore_system(restore);
        assert!(vars.prepared_plan_cache_enabled());

        let globals = GlobalSysvars::new();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE,
                "OFF".to_owned(),
            )
            .unwrap();
        let mut inherited = SessionVars::new();
        inherited.seed_from_globals(globals).unwrap();
        assert!(!inherited.prepared_plan_cache_enabled());
    }

    #[test]
    fn shared_lock_upgrade_switch_uses_go_typed_state() {
        let mut vars = SessionVars::new();
        assert!(!vars.shared_lock_upgrade_enabled());
        let restore = vars.snapshot_system(tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE);
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE,
            "ON".to_owned(),
        )
        .unwrap();
        assert!(vars.shared_lock_upgrade_enabled());
        vars.restore_system(restore);
        assert!(!vars.shared_lock_upgrade_enabled());

        let globals = GlobalSysvars::new();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE,
                "ON".to_owned(),
            )
            .unwrap();
        vars.seed_from_globals(globals).unwrap();
        assert!(vars.shared_lock_upgrade_enabled());
    }

    #[test]
    fn defaults_come_from_the_registry() {
        let vars = SessionVars::new();
        assert_eq!(vars.get_system("autocommit").unwrap(), "ON");
        assert_eq!(vars.get_system("character_set_client").unwrap(), "utf8mb4");
        assert_eq!(
            vars.get_system("transaction_isolation").unwrap(),
            "REPEATABLE-READ"
        );
        assert_eq!(vars.get_system("max_allowed_packet").unwrap(), "67108864");
        // The lookup is case-insensitive, as Go's is after lowercasing.
        assert_eq!(vars.get_system("AUTOCOMMIT").unwrap(), "ON");
    }

    #[test]
    fn sem_enable_and_disable_change_new_session_defaults() {
        struct DisableSemOnDrop;

        impl Drop for DisableSemOnDrop {
            fn drop(&mut self) {
                tidb_util::sem::disable();
            }
        }

        tidb_util::sem::disable();
        let _reset = DisableSemOnDrop;

        tidb_util::sem::enable();
        let enabled = SessionVars::new();
        assert_eq!(
            enabled.get_system("tidb_enable_enhanced_security").unwrap(),
            "ON"
        );
        assert_eq!(enabled.get_system("hostname").unwrap(), "localhost");

        let mut recovery = crate::Session::new();
        recovery.set_user("recovery@%".to_owned(), "recovery@127.0.0.1".to_owned());
        recovery.attach_privileges(crate::privilege::PrivilegeRegistry::default());
        assert!(recovery.sem_hides_status_var("tidb_gc_leader_desc"));
        recovery.enable_privilege_bypass();
        assert!(
            !recovery.sem_hides_status_var("tidb_gc_leader_desc"),
            "skip-grant-table satisfies RESTRICTED_STATUS_ADMIN verification",
        );

        tidb_util::sem::disable();
        let disabled = SessionVars::new();
        assert_eq!(
            disabled
                .get_system("tidb_enable_enhanced_security")
                .unwrap(),
            "OFF"
        );
    }

    #[test]
    fn an_unknown_system_variable_is_rejected_both_ways() {
        let mut vars = SessionVars::new();
        assert_eq!(
            vars.get_system("nope"),
            Err(VarError::UnknownSystemVariable("nope".to_owned()))
        );
        assert_eq!(
            vars.set_system("nope", "1".to_owned()),
            Err(VarError::UnknownSystemVariable("nope".to_owned()))
        );
    }

    #[test]
    fn a_loaded_row_overrides_and_an_absent_one_still_falls_back_to_default() {
        let globals = GlobalSysvars::new();
        globals.load_from_cluster([("AUTOCOMMIT".to_owned(), "OFF".to_owned())]);
        // The seeded row overrides, case-insensitively, exactly like `set`.
        assert_eq!(globals.get("autocommit").unwrap(), "OFF");
        // A variable the fixture never mentioned still falls back to the
        // registry default -- `load_from_cluster` only overwrites names it is
        // given.
        assert_eq!(globals.get("max_allowed_packet").unwrap(), "67108864");
    }

    #[test]
    fn a_loaded_row_naming_an_unknown_variable_is_skipped_not_refused() {
        let globals = GlobalSysvars::new();
        // Bypasses `set`'s validation on purpose (a stored row already passed
        // it once), but an unrecognized name from an older/newer cluster must
        // still not panic or poison every other loaded row.
        globals.load_from_cluster([
            ("not_a_real_variable".to_owned(), "x".to_owned()),
            ("autocommit".to_owned(), "OFF".to_owned()),
        ]);
        assert_eq!(
            globals.get("not_a_real_variable"),
            Err(VarError::UnknownSystemVariable(
                "not_a_real_variable".to_owned()
            ))
        );
        assert_eq!(globals.get("autocommit").unwrap(), "OFF");
    }

    #[test]
    fn a_read_only_variable_cannot_be_set() {
        let mut vars = SessionVars::new();
        assert!(vars.get_system("version").unwrap().starts_with("8.0.11-"));
        assert_eq!(
            vars.set_system("version", "9".to_owned()),
            Err(VarError::ReadOnlyVariable("version".to_owned()))
        );
    }

    #[test]
    fn set_overrides_the_default_and_set_names_moves_three_variables() {
        let mut vars = SessionVars::new();
        vars.set_system("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("autocommit").unwrap(), "OFF");

        vars.set_names("latin1", Some("latin1_bin")).unwrap();
        for name in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            assert_eq!(vars.get_system(name).unwrap(), "latin1", "{name}");
        }
        assert_eq!(
            vars.get_system("collation_connection").unwrap(),
            "latin1_bin"
        );
        // SET NAMES leaves the server-side character set alone.
        assert_eq!(vars.get_system("character_set_server").unwrap(), "utf8mb4");
    }

    #[test]
    fn set_global_writes_the_shared_table_not_this_sessions_own_copy() {
        let mut vars = SessionVars::new();
        // `autocommit` (scope 3: both SESSION and GLOBAL) is the case Go
        // documents: `SET GLOBAL` never moves the setting session's own
        // `@@autocommit`.
        vars.set_global("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("autocommit").unwrap(), "ON");
        assert_eq!(vars.get_global("autocommit").unwrap(), "OFF");
    }

    #[test]
    fn set_global_redact_log_updates_the_process_redaction_authority() {
        let mut vars = SessionVars::new();
        vars.set_global("tidb_redact_log", "MARKER".to_owned())
            .unwrap();
        assert!(tidb_util::redact::need_redact());
        assert_eq!(tidb_util::redact::value("secret"), "?");

        vars.globals.reset("tidb_redact_log").unwrap();
        assert!(!tidb_util::redact::need_redact());

        let scratch =
            GlobalSysvars::from_cluster_rows([("tidb_redact_log".to_owned(), "MARKER".to_owned())]);
        assert!(!tidb_util::redact::need_redact());
        vars.globals.replace_from(&scratch);
        assert!(tidb_util::redact::need_redact());

        vars.set_global("tidb_redact_log", "OFF".to_owned())
            .unwrap();
        assert!(!tidb_util::redact::need_redact());

        vars.globals
            .load_from_cluster([("tidb_redact_log".to_owned(), "ON".to_owned())]);
        assert!(tidb_util::redact::need_redact());
        vars.globals.reset("tidb_redact_log").unwrap();
    }

    #[test]
    fn global_memory_arbitration_settings_update_the_running_process_authority() {
        struct NoopRecorder;

        impl tidb_util::memory::RecordMemState for NoopRecorder {
            fn load(&self) -> Result<Option<tidb_util::memory::RuntimeMemStateV1>, String> {
                Ok(None)
            }

            fn store(&self, _: &tidb_util::memory::RuntimeMemStateV1) -> Result<(), String> {
                Ok(())
            }
        }

        let arbitrator =
            tidb_util::memory::MemArbitrator::new(1 << 30, 4, 4, 64 << 10, Box::new(NoopRecorder));
        let _registration = tidb_util::memory::install_process_arbitrator(&arbitrator);
        let globals = GlobalSysvars::new();

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE,
                "priority".to_owned(),
            )
            .unwrap();
        assert_eq!(
            arbitrator.work_mode(),
            tidb_util::memory::ArbitratorWorkMode::Priority
        );

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT,
                "0.5".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.soft_limit(), 512 << 20);

        globals
            .set_instance(
                tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
                "2GiB".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.limit_u64(), 2 << 30);

        globals
            .reset(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE)
            .unwrap();
        assert_eq!(
            arbitrator.work_mode(),
            tidb_util::memory::ArbitratorWorkMode::Disable
        );
    }

    #[test]
    fn set_global_on_a_session_only_variable_is_1228() {
        let mut vars = SessionVars::new();
        // `error_count` is SESSION-only (scope 2): Go's `ErrLocalVariable`.
        assert_eq!(
            vars.set_global("debug_sync", "x".to_owned()),
            Err(VarError::SessionOnlyVariable("debug_sync".to_owned()))
        );
    }

    #[test]
    fn replacing_a_cluster_global_image_preserves_instance_only_values() {
        let globals = GlobalSysvars::new();
        globals
            .set_instance("tidb_general_log", "ON".to_owned())
            .expect("the instance-only variable sets");
        globals
            .set("autocommit", "OFF".to_owned())
            .expect("the old global override sets");

        let fresh = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        globals.replace_from(&fresh);

        assert_eq!(globals.get("tidb_general_log").as_deref(), Ok("ON"));
        assert_eq!(globals.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(globals.get("autocommit").as_deref(), Ok("ON"));
    }

    #[test]
    fn set_session_on_a_global_only_variable_is_1229() {
        let mut vars = SessionVars::new();
        // `default_password_lifetime` is GLOBAL-only (scope 1): Go's
        // `ErrGlobalVariable`.
        assert_eq!(
            vars.set_system("default_password_lifetime", "5".to_owned()),
            Err(VarError::GlobalOnlyVariable(
                "default_password_lifetime".to_owned()
            ))
        );
    }

    #[test]
    fn reading_global_on_a_session_only_variable_is_1238() {
        let vars = SessionVars::new();
        assert_eq!(
            vars.get_global("debug_sync"),
            Err(VarError::NoGlobalCopy("debug_sync".to_owned()))
        );
    }

    #[test]
    fn a_new_session_inherits_the_global_but_a_live_session_does_not() {
        let globals = GlobalSysvars::new();

        // A session already open BEFORE the GLOBAL changes keeps its own
        // value -- MySQL's rule that the session copy is made once, at
        // connect.
        let mut live = SessionVars::new();
        live.seed_from_globals(globals.clone()).unwrap();
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "ON");

        // `SET GLOBAL` again, then a brand NEW session opens: it inherits
        // the value that was live at ITS connect time.
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        let mut fresh = SessionVars::new();
        fresh.seed_from_globals(globals).unwrap();
        assert_eq!(fresh.get_system("autocommit").unwrap(), "OFF");

        // Neither session's inherited copy is the live table: further
        // session-only `SET` traffic on one does not leak to the other.
        live.set_system("autocommit", "OFF".to_owned()).unwrap();
        fresh.set_system("autocommit", "ON".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "OFF");
        assert_eq!(fresh.get_system("autocommit").unwrap(), "ON");
    }
}

#[cfg(test)]
mod mview_from_job_tests {
    use super::*;
    use std::collections::BTreeMap;
    use tidb_model::GoShared;

    #[test]
    fn mview_execution_vars_reconstruct_from_job_envelope() {
        let mut job_vars = BTreeMap::new();
        job_vars.insert(
            "tidb_mview_maintain_mem_quota".to_owned(),
            tidb_datatype::GoString::from("123"),
        );
        job_vars.insert(
            "tidb_mview_maintain_isolation_read_engines".to_owned(),
            tidb_datatype::GoString::from("tikv"),
        );
        job_vars.insert(
            "tidb_max_tiflash_threads".to_owned(),
            tidb_datatype::GoString::from("8"),
        );
        let mut job = Job::default();
        job.session_vars = Some(GoShared::new(job_vars));
        let restored = m_view_execution_session_vars_from_job(Some(&job), &SessionVars::default());
        assert_eq!(restored.maintain_mem_quota, 123);
        assert_eq!(restored.isolation_read_engines, "tikv");
        assert_eq!(restored.ti_flash_max_threads, 8);
        // Fields absent from the envelope keep the captured defaults.
        // Fields absent from the envelope keep the captured defaults.
        let defaults = m_view_execution_session_vars_from_job(None, &SessionVars::default());
        assert_eq!(
            restored.fine_grained_batch_size, defaults.fine_grained_batch_size,
            "absent envelope fields keep the captured default"
        );
    }
}
