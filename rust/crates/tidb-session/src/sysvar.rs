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

//! The system-variable registry: Go `pkg/sessionctx/variable`'s `sysVars`
//! table plus the value validation `SysVar.ValidateFromType` performs.
//!
//! GENERATED, NOT HAND-WRITTEN: 961 base entries were captured from
//! `sysvar.go`; the four workload-repository entries registered by Go's
//! `pkg/util/workloadrepo.init` are merged into the same runtime registry.
//! captured from this repository's own Go registry by iterating
//! `variable.GetSysVars()` and printing each entry's name, scope, default,
//! type, read-only flag, bounds and possible values. Regexing `sysvar.go`
//! would have been wrong -- many defaults are computed (`strconv.Format...`,
//! config lookups) and `TypeStr` is 4 rather than 0 because its `iota` block
//! starts after the scope constants -- so the values here are what a running
//! TiDB actually reports.
//!
//! Go's `TypeFlag` zero value and its explicit `TypeStr` both fall through
//! the validation switch to the string case, so both map to [`VarType::Str`].
//!
//! A clamp is not a refusal: every check that narrows a value instead of
//! rejecting it records [`Validated::truncated`], and the `SET` path turns
//! that into Go's `ErrTruncatedWrongValue` (1292) naming the ORIGINAL text
//! (see `Session::warn_truncated_var`).
//!
//! NOT MODELLED (documented): the per-variable `Validation` closures other
//! than those [`SysVarDef::run_validation`] names, and the `SetSession` and
//! `GetSession` closures Go attaches to many entries (charset name checks,
//! isolation-level checks, autocommit's implicit commit, and every variable
//! whose read is computed rather than stored); instance-specific mutation
//! hooks beyond the explicit read-tier routing; and the global tier's
//! persistence. The table's declarative part -- names, scopes, defaults,
//! types, bounds, enums, read-only -- is complete. The one registry flag the
//! capture does not expose, `InternalSessionVariable`, is retained by
//! [`SysVarDef::is_internal_session_variable`].

/// Go `vardef.ScopeNone`: a read-only server property.
pub const SCOPE_NONE: u8 = 0;

/// Go `pkg/sessionctx/variable/removed.go`'s compatibility table. Removed
/// names remain accepted by `SET` (the session writer checks this table before
/// normal registry lookup), while reads report why the option disappeared.
const REMOVED_SYS_VARS: &[(&str, &str)] = &[
    (
        "tidb_enable_alter_placement",
        "alter placement is now always enabled",
    ),
    (
        "tidb_enable_global_temporary_table",
        "temporary table support is now always enabled",
    ),
    ("tidb_slow_log_masking", "use tidb_redact_log instead"),
    (
        "placement_checks",
        "placement_checks is removed and use tidb_placement_mode instead",
    ),
    (
        "tidb_mem_quota_hashjoin",
        "use tidb_mem_quota_query instead",
    ),
    (
        "tidb_mem_quota_mergejoin",
        "use tidb_mem_quota_query instead",
    ),
    ("tidb_mem_quota_sort", "use tidb_mem_quota_query instead"),
    ("tidb_mem_quota_topn", "use tidb_mem_quota_query instead"),
    (
        "tidb_mem_quota_indexlookupreader",
        "use tidb_mem_quota_query instead",
    ),
    (
        "tidb_mem_quota_indexlookupjoin",
        "use tidb_mem_quota_query instead",
    ),
    ("tidb_enable_streaming", "streaming is no longer supported"),
    (
        "tidb_opt_broadcast_join",
        "tidb_opt_broadcast_join is removed and use tidb_allow_mpp instead",
    ),
    (
        "tidb_enable_change_multi_schema",
        "alter multiple schema objects in a table is now always enabled",
    ),
];

/// Go's `noopSysVars` entries whose `IsNoop` bit affects session seeding. The
/// full compatibility catalog remains in the generated registry; these are
/// the entries exercised by the executable parity tests and by the no-op gate.
const NOOP_SYS_VARS: &[&str] = &[
    "tx_read_only",
    "transaction_read_only",
    "offline_mode",
    "super_read_only",
    "read_only",
    "innodb_fast_shutdown",
];

/// Returns Go's removal explanation for a system-variable name, matching the
/// case-insensitive lookup performed by `GetSysVar` and `IsRemovedSysVar`.
pub fn removed_sys_var_reason(name: &str) -> Option<&'static str> {
    REMOVED_SYS_VARS
        .iter()
        .find(|(removed, _)| removed.eq_ignore_ascii_case(name))
        .map(|(_, reason)| *reason)
}

/// Go `IsRemovedSysVar`.
#[must_use]
pub fn is_removed_sys_var(name: &str) -> bool {
    removed_sys_var_reason(name).is_some()
}
/// Go `vardef.ScopeGlobal`.
pub const SCOPE_GLOBAL: u8 = 1;
/// Go `vardef.ScopeSession`.
pub const SCOPE_SESSION: u8 = 2;
/// Go `vardef.ScopeInstance`.
pub const SCOPE_INSTANCE: u8 = 4;

/// Go `vardef.TypeFlag`, restricted to the cases the validation switch
/// distinguishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VarType {
    /// Go's `TypeStr` and the unset zero value.
    Str,
    /// Go `TypeBool`.
    Bool,
    /// Go `TypeInt`.
    Int,
    /// Go `TypeEnum`.
    Enum,
    /// Go `TypeFloat`.
    Float,
    /// Go `TypeUnsigned`.
    Unsigned,
    /// Go `TypeTime`.
    Time,
    /// Go `TypeDuration`.
    Duration,
}

/// One entry of Go's `sysVars` table.
#[derive(Clone, Copy, Debug)]
pub struct SysVarDef {
    /// The variable name, always lowercase in Go's registry.
    pub name: &'static str,
    /// Go `Scope`, a bit set of the `SCOPE_*` flags.
    pub scope: u8,
    /// Go `Value`: the default.
    pub value: &'static str,
    /// Go `Type`.
    pub var_type: VarType,
    /// Go `ReadOnly`.
    pub read_only: bool,
    /// Go `AllowAutoValue`: `-1` is accepted even outside the range.
    pub allow_auto_value: bool,
    /// Go `MinValue`.
    pub min_value: i64,
    /// Go `MaxValue`.
    pub max_value: u64,
    /// Go `PossibleValues`, for `TypeEnum`.
    pub possible_values: &'static [&'static str],
    /// Go `AutoConvertNegativeBool`.
    pub auto_convert_negative_bool: bool,
}

impl SysVarDef {
    /// A fill value for the catalog merge's output array before every slot is
    /// written. It is never observed: the merge writes exactly `TOTAL` slots.
    pub(crate) const PLACEHOLDER: Self = Self {
        name: "",
        scope: 0,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    };

    /// Go `HasSessionScope`.
    #[must_use]
    pub fn has_session_scope(&self) -> bool {
        self.scope & SCOPE_SESSION != 0
    }

    /// Go `HasGlobalScope`.
    #[must_use]
    pub fn has_global_scope(&self) -> bool {
        self.scope & SCOPE_GLOBAL != 0
    }

    /// Go `HasInstanceScope`.
    #[must_use]
    pub fn has_instance_scope(&self) -> bool {
        self.scope & SCOPE_INSTANCE != 0
    }

    /// Go `validateScope`'s read-only test: an explicitly read-only variable,
    /// or one with no scope at all.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.read_only || self.scope == SCOPE_NONE
    }

    /// Go `InternalSessionVariable`: an explicit `@@session.x` must hide the
    /// variable even though an unqualified internal read remains available.
    ///
    /// The source registry has exactly one such entry. Keep it here rather
    /// than adding a generated field to all 961 entries for one true value.
    #[must_use]
    pub fn is_internal_session_variable(&self) -> bool {
        self.name == "tidb_redact_log"
    }

    /// Go `SysVar.IsNoop`: compatibility-only variables retain their value in
    /// the global registry but skip the initial session copy.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        NOOP_SYS_VARS.contains(&self.name)
    }
}

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use chrono::{Offset, TimeZone};
use tidb_error::mysql::{FormatArg, SqlError};

mod catalog;

pub use catalog::SYS_VARS;

/// The name to look a variable up by, lowercased only when it needs it.
///
/// Statement execution reads variables by the dozens, and almost every name
/// that arrives is already lowercase -- parser output and internal callers
/// both speak in the registry's own casing. `to_ascii_lowercase()` allocates
/// for every one of them; the scan proves most calls need no copy at all.
pub(crate) fn lowered_if_needed(name: &str) -> std::borrow::Cow<'_, str> {
    if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
        std::borrow::Cow::Owned(name.to_ascii_lowercase())
    } else {
        std::borrow::Cow::Borrowed(name)
    }
}

/// Go `GetSysVar`: looks an entry up by name, case-insensitively (Go
/// lowercases first).
///
/// Go resolves the name through the `sysVars` map, one hash probe. The name-
/// ordered [`SYS_VARS`] slice stays as the const-time assembly and the test
/// oracle, but reads go through a lazily-built name -> index table: statement
/// execution looks variables up by the dozens, and a binary search over ~950
/// entries pays a string compare per level every single time.
#[must_use]
pub fn get_sys_var(name: &str) -> Option<&'static SysVarDef> {
    sys_var_index_lookup(name).map(|index| &SYS_VARS[index])
}

fn sem_v2_defaults() -> &'static RwLock<HashMap<String, String>> {
    static DEFAULTS: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    DEFAULTS.get_or_init(|| RwLock::new(HashMap::new()))
}

struct SemV2SysVarRegistry;

impl tidb_util::sem_v2::SysVarRegistry for SemV2SysVarRegistry {
    fn get_sys_var(&self, name: &str) -> Option<tidb_util::sem_v2::SysVar> {
        let definition = get_sys_var(name)?;
        let scope = match definition.scope {
            SCOPE_NONE => tidb_util::sem_v2::SysVarScope::None,
            SCOPE_GLOBAL => tidb_util::sem_v2::SysVarScope::Global,
            SCOPE_SESSION => tidb_util::sem_v2::SysVarScope::Session,
            SCOPE_INSTANCE => tidb_util::sem_v2::SysVarScope::Instance,
            _ => tidb_util::sem_v2::SysVarScope::Other,
        };
        Some(tidb_util::sem_v2::SysVar {
            scope,
            value: effective_default(definition),
        })
    }

    fn set_sys_var(&self, name: &str, value: &str) {
        let Some(definition) = get_sys_var(name) else {
            return;
        };
        sem_v2_defaults()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(definition.name.to_owned(), value.to_owned());
    }
}

/// Installs Go `variable.GetSysVar` / `SetSysVar` as SEM v2's registry.
pub fn install_sem_v2_sysvar_registry() {
    tidb_util::sem_v2::set_sys_var_registry(Some(Arc::new(SemV2SysVarRegistry)));
}

/// The registry position of the entry `name` addresses (case-insensitively),
/// for callers that want the slot itself -- the global-variable snapshot is
/// indexed by registry position so a read needs no second string probe.
#[must_use]
pub(crate) fn sys_var_index_lookup(name: &str) -> Option<usize> {
    let lowered = lowered_if_needed(name);
    sys_var_index().get(lowered.as_ref()).copied()
}

/// The registry's names are unique (the sortedness test rejects duplicates),
/// so a hash table keyed by the entry's own name answers exactly what the old
/// binary search answered -- just without the per-probe comparisons.
fn sys_var_index() -> &'static std::collections::HashMap<&'static str, usize> {
    static INDEX: std::sync::OnceLock<std::collections::HashMap<&'static str, usize>> =
        std::sync::OnceLock::new();
    INDEX.get_or_init(|| {
        SYS_VARS
            .iter()
            .enumerate()
            .map(|(index, definition)| (definition.name, index))
            .collect()
    })
}

/// The process-effective default for one registry entry.
///
/// Most defaults are immutable captured source values. `pkg/util/sem` owns
/// the two exceptions Go mutates through `variable.SetSysVar` when SEM is
/// enabled or disabled.
#[must_use]
pub fn effective_default(definition: &SysVarDef) -> String {
    effective_default_value(definition).into_owned()
}

/// The process-effective default without allocating for the ordinary static
/// registry case. Go returns a string header from `SysVar.Value`; Rust callers
/// that only inspect a value should not have to clone its backing bytes.
#[must_use]
pub fn effective_default_value(definition: &SysVarDef) -> Cow<'static, str> {
    if let Some(value) = sem_v2_defaults()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(definition.name)
        .cloned()
    {
        return Cow::Owned(value);
    }
    tidb_util::sem::effective_sysvar_default(definition.name)
        .map_or_else(|| Cow::Borrowed(definition.value), Cow::Owned)
}

/// Go `SysVar.AllowEmpty`: the empty string means "read the value from the
/// config file", and is accepted only for `SET SESSION`.
///
/// HAND-MAINTAINED, NOT GENERATED: the table above was captured from a running
/// registry through `GetSysVars()`, which does not expose these two flags, so
/// the exact `AllowEmpty: true` / `AllowEmptyAll: true` name sets are
/// transcribed from `pkg/sessionctx/variable/sysvar.go`.
const ALLOW_EMPTY_VARS: &[&str] = &[
    "identity",
    "last_insert_id",
    "tidb_current_ts",
    "tidb_enable_top_sql",
    "tidb_enable_stmt_summary",
    "tidb_read_staleness",
    "tidb_schema_version_cache_limit",
    "tidb_stmt_summary_history_size",
    "tidb_stmt_summary_internal_query",
    "tidb_stmt_summary_max_sql_length",
    "tidb_stmt_summary_max_stmt_count",
    "tidb_stmt_summary_refresh_interval",
    "tidb_stmt_summary_persist_evicted",
    "tidb_stmt_summary_group_by_user",
    "tidb_exp_embed_openai_api_base",
];

/// Go `SysVar.AllowEmptyAll`: the empty string is accepted in every scope.
///
/// Go's own comment names exactly these two variables. `tidb_txn_mode = ''`
/// is the one that matters for transactions: an empty mode is neither
/// `pessimistic` nor a rejected enum value -- Go's `decideTxnMode` reads
/// anything other than `pessimistic` as optimistic.
const ALLOW_EMPTY_ALL_VARS: &[&str] = &["tidb_capture_plan_baselines", "tidb_txn_mode"];

/// Go `SysVar.Aliases`: writing one of these names writes the other too, so
/// the pair is a single value under two spellings. Go applies the alias in
/// `SetSessionFromHook`/`SetGlobalFromHook` AFTER validation and skips the
/// alias's own validation, which is why the stored form is simply copied.
///
/// HAND-MAINTAINED for the same reason as the tables above: `GetSysVars()`
/// does not expose the field. Every reciprocal pair in the registry is here --
/// two from `sysvar.go` (`tx_isolation`, the plan-cache size) and one from
/// `noop.go` (`tx_read_only`) -- each listed in both directions.
const ALIASES: &[(&str, &str)] = &[
    (
        "tidb_prepared_plan_cache_size",
        "tidb_session_plan_cache_size",
    ),
    (
        "tidb_session_plan_cache_size",
        "tidb_prepared_plan_cache_size",
    ),
    ("transaction_isolation", "tx_isolation"),
    ("tx_isolation", "transaction_isolation"),
    ("transaction_read_only", "tx_read_only"),
    ("tx_read_only", "transaction_read_only"),
];

/// Every `noop.go` variable whose `Validation` refuses an ON value unless
/// `tidb_enable_noop_functions` allows it, paired with the clause name its
/// 1235 diagnostic uses.
///
/// The five read-only ones share `varsutil.go:checkReadOnly`, which picks
/// `OFFLINE MODE` or `READ ONLY` from its `offlineMode` argument.
/// `sql_auto_is_null` carries the SAME logic inline in its own registration
/// -- same three-way branch, same refusal to `Off`, same warning in `WARN`
/// mode -- and differs only in naming ITSELF as the clause. It belongs here
/// rather than in a sixth copy of the rule.
///
/// HAND-MAINTAINED for the same reason as the tables above: the registry the
/// generator reads does not expose `Validation`.
const NOOP_GATED_VARS: &[(&str, &str)] = &[
    ("tx_read_only", "READ ONLY"),
    ("transaction_read_only", "READ ONLY"),
    ("offline_mode", "OFFLINE MODE"),
    ("super_read_only", "READ ONLY"),
    ("read_only", "READ ONLY"),
    ("sql_auto_is_null", "sql_auto_is_null"),
];

/// The clause name the `tidb_enable_noop_functions` gate would put in its
/// 1235 diagnostic for `name`, or `None` when `name`'s `Validation` does not
/// consult that gate.
#[must_use]
pub fn noop_gated_clause(name: &str) -> Option<&'static str> {
    NOOP_GATED_VARS
        .iter()
        .find(|(candidate, _)| name.eq_ignore_ascii_case(candidate))
        .map(|&(_, clause)| clause)
}

/// The other spelling of `name`, if it has one.
#[must_use]
pub fn alias_of(name: &str) -> Option<&'static str> {
    ALIASES
        .iter()
        .find(|(from, _)| *from == name)
        .map(|(_, to)| *to)
}

/// Why a value was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationError {
    /// Go `ErrWrongTypeForVar` (1232).
    WrongType,
    /// Go `ErrWrongValueForVar` (1231).
    WrongValue,
    /// A catalogued MySQL error returned directly by a `Validation` closure.
    SqlError(tidb_error::mysql::SqlError),
    /// A `Validation` closure that refuses the value with a bare
    /// `errors.Errorf`, whose wording IS the error (Go gives it no code, so it
    /// reports as 1105).
    Refused(String),
}

/// The outcome of validating a value: Go returns the (possibly normalized)
/// value together with a warning it appends to the statement context, so both
/// travel here rather than the warning being dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Validated {
    /// The value to store, which may be clamped or canonicalized.
    pub value: String,
    /// Whether Go appended `ErrTruncatedWrongValue` for an out-of-range input.
    pub truncated: bool,
}

/// Go `checkTimeSystemVar`, using the supplied session location for a short
/// `HH:MM` value and preserving an explicitly supplied numeric offset. The
/// registry-level validator uses UTC; GLOBAL SQL writes call this helper again
/// with the issuing session's location before the value reaches the hook.
pub(crate) fn normalize_time_value(
    value: &str,
    zone: &tidb_executor::SessionTimeZone,
) -> Result<String, ValidationError> {
    let mut fields = value.split_whitespace();
    let clock = fields.next().ok_or(ValidationError::WrongType)?;
    let explicit_offset = fields.next();
    if fields.next().is_some() {
        return Err(ValidationError::WrongType);
    }
    let (hour, minute) = clock.split_once(':').ok_or(ValidationError::WrongType)?;
    let hour = hour.parse::<u8>().map_err(|_| ValidationError::WrongType)?;
    let minute = minute
        .parse::<u8>()
        .map_err(|_| ValidationError::WrongType)?;
    if hour >= 24 || minute >= 60 {
        return Err(ValidationError::WrongType);
    }
    let offset_minutes = if let Some(offset) = explicit_offset {
        let bytes = offset.as_bytes();
        if bytes.len() != 5 || !matches!(bytes[0], b'+' | b'-') {
            return Err(ValidationError::WrongType);
        }
        let hours = std::str::from_utf8(&bytes[1..3])
            .ok()
            .and_then(|text| text.parse::<u16>().ok())
            .ok_or(ValidationError::WrongType)?;
        let minutes = std::str::from_utf8(&bytes[3..5])
            .ok()
            .and_then(|text| text.parse::<u8>().ok())
            .ok_or(ValidationError::WrongType)?;
        if hours >= 24 || minutes >= 60 {
            return Err(ValidationError::WrongType);
        }
        let total = i32::from(hours) * 60 + i32::from(minutes);
        if bytes[0] == b'-' {
            -total
        } else {
            total
        }
    } else {
        let now = chrono::Utc::now().naive_utc();
        zone.offset_from_utc_datetime(&now).fix().local_minus_utc() / 60
    };
    let sign = if offset_minutes < 0 { '-' } else { '+' };
    let absolute = offset_minutes.unsigned_abs();
    Ok(format!(
        "{hour:02}:{minute:02} {sign}{:02}{:02}",
        absolute / 60,
        absolute % 60
    ))
}

/// Go `normalizeIsolationReadEnginesValue` (shared by
/// `tidb_isolation_read_engines` and
/// `tidb_mview_maintain_isolation_read_engines`): comma-split engines trim,
/// canonicalize case-insensitively to tikv/tiflash/tidb, and an empty or
/// unknown engine refuses the SET with `ErrWrongValueForVar`.
pub(crate) fn normalize_isolation_read_engines_value(normalized: &str) -> Result<String, ()> {
    let engines = normalized.split(',');
    let mut formatted = String::new();
    for (index, engine) in engines.enumerate() {
        let engine = engine.trim();
        if engine.is_empty() {
            return Err(());
        }
        if index != 0 {
            formatted.push(',');
        }
        if engine.eq_ignore_ascii_case("tikv") {
            formatted.push_str("tikv");
        } else if engine.eq_ignore_ascii_case("tiflash") {
            formatted.push_str("tiflash");
        } else if engine.eq_ignore_ascii_case("tidb") {
            formatted.push_str("tidb");
        } else {
            return Err(());
        }
    }
    Ok(formatted)
}

impl SysVarDef {
    /// Go `SysVar.ValidateFromType`: the type-directed value check, returning
    /// the normalized value.
    ///
    /// Go's per-variable `Validation` closure runs after this and is not
    /// modelled (see the module doc).
    ///
    /// Validates as `SET SESSION` does; [`Self::validate_in_scope`] is the
    /// form that distinguishes the scopes.
    pub fn validate(&self, value: &str) -> Result<Validated, ValidationError> {
        self.validate_in_scope(value, SCOPE_SESSION)
    }

    /// Go `SysVar.ValidateWithRelaxedValidation`: normalize values when
    /// possible, but squash both type and variable-specific validation errors
    /// and return the caller's original text when validation refuses it.
    #[must_use]
    pub fn validate_with_relaxed_validation(&self, value: &str, scope: u8) -> String {
        self.validate_in_scope(value, scope)
            .map_or_else(|_| value.to_owned(), |validated| validated.value)
    }

    /// Go `SysVar.ValidateFromType` including its `scope` argument, which only
    /// the empty-value escape hatch reads.
    pub fn validate_in_scope(&self, value: &str, scope: u8) -> Result<Validated, ValidationError> {
        self.validate_in_scope_with_lookup(value, scope, None)
    }

    /// `validate_in_scope` plus an optional reader for sibling sysvars'
    /// current values, for hooks Go resolves against `vars.systems` (e.g.
    /// `checkIsolationLevel` reading `tidb_skip_isolation_level_check`).
    /// `None` answers as if every sibling were unset.
    pub fn validate_in_scope_with_lookup(
        &self,
        value: &str,
        scope: u8,
        lookup: Option<&dyn Fn(&str) -> Option<String>>,
    ) -> Result<Validated, ValidationError> {
        // Go's tidb_gogc_tuner_threshold Validation (`sysvar.go:1270`)
        // consumes the RAW value before any type normalization: a
        // non-numeric input silently falls back to the default 0.6
        // (`tidbOptFloat64`), numbers store as their shortest float text,
        // and the two range guards are dead (an `&&` over contradictory
        // predicates) plus a runtime-tuner comparison whose tuner reads 0
        // until startup sets it, so neither can reject here.
        if self.name == "tidb_gogc_tuner_threshold" {
            let float_value = value.parse::<f64>().unwrap_or(0.6);
            return Ok(Validated {
                value: format!("{float_value}"),
                truncated: false,
            });
        }
        let validated = self.normalize_by_type(value, scope)?;
        self.run_validation_with_lookup(validated, value, lookup)
    }

    /// Go `ValidateFromType` ALONE, without the per-variable `Validation`
    /// closure that runs after it. This is the `normalizedValue` Go hands that
    /// closure, and the value the closure's own warnings are decided on -- for
    /// `tidb_enable_table_partition` the two differ by construction, since the
    /// closure stores `ON` for the very assignment (`OFF`) it warns about.
    pub fn normalize_by_type(&self, value: &str, scope: u8) -> Result<Validated, ValidationError> {
        if value.is_empty() && self.allows_empty_value(scope) {
            return Ok(Validated {
                value: String::new(),
                truncated: false,
            });
        }
        match self.var_type {
            VarType::Unsigned => self.check_uint64(value),
            VarType::Int => self.check_int64(value),
            VarType::Bool => self.check_bool(value),
            VarType::Float => self.check_float(value),
            VarType::Enum => self.check_enum(value),
            VarType::Time => Ok(Validated {
                value: normalize_time_value(value, &tidb_executor::SessionTimeZone::utc())?,
                truncated: false,
            }),
            VarType::Duration => self.check_duration(value),
            VarType::Str => Ok(Validated {
                value: value.to_owned(),
                truncated: false,
            }),
        }
    }

    /// Go's per-variable `SysVar.Validation` closure, which runs after
    /// `ValidateFromType` and returns the value actually stored.
    ///
    /// Only the variables whose stored form differs from what the user typed
    /// are here; everything else takes the type-validated value unchanged.
    /// The point of doing it at SET time is that every reader afterwards sees
    /// one canonical form, so no read site has to re-expand anything.
    ///
    /// `original` is the value as TYPED. Go hands the closure both the
    /// normalized and the original text, and `timestamp`'s closure reads the
    /// original -- so a value the type check would silently clamp is still
    /// rejected.
    fn run_validation(
        &self,
        validated: Validated,
        original: &str,
    ) -> Result<Validated, ValidationError> {
        self.run_validation_with_lookup(validated, original, None)
    }

    fn run_validation_with_lookup(
        &self,
        validated: Validated,
        original: &str,
        lookup: Option<&dyn Fn(&str) -> Option<String>>,
    ) -> Result<Validated, ValidationError> {
        // Go's `timestamp` validation (`sysvar.go`, the `vardef.Timestamp`
        // entry): `tidbOptFloat64(originalValue)` above `math.MaxInt32` is
        // `ErrWrongValueForVar`. The type check alone would have clamped it to
        // the bound and accepted it, which is the opposite outcome.
        if self.name == "timestamp"
            && original
                .parse::<f64>()
                .is_ok_and(|value| value > f64::from(i32::MAX))
        {
            return Err(ValidationError::WrongValue);
        }
        // Go's `time_zone` validation (`sysvar.go`, the `vardef.TimeZone`
        // entry): `SYSTEM` in any spelling canonicalizes to upper case, and
        // every other name is stored exactly as typed -- `SET time_zone='utc'`
        // reads back `utc`, not `UTC`.
        if self.name == "time_zone" {
            if validated.value.eq_ignore_ascii_case("SYSTEM") {
                return Ok(Validated {
                    value: "SYSTEM".to_owned(),
                    truncated: validated.truncated,
                });
            }
            tidb_util::timeutil::parse_time_zone(&validated.value)
                .map_err(|error| ValidationError::SqlError(error.to_sql_error()))?;
            return Ok(validated);
        }
        // Go's `secure_auth` validation keeps the deprecated compatibility
        // switch permanently enabled: OFF is ErrWrongValueForVar (1231),
        // while ON remains the normalized stored value.
        if self.name == "secure_auth" && validated.value == "OFF" {
            return Err(ValidationError::WrongValue);
        }
        // Go's `tidb_mpp_store_fail_ttl` compatibility variable is always
        // forced back to its `0s` default. Its Session hook emits a warning,
        // while the value returned by Validation is what the table stores.
        if self.name == "tidb_mpp_store_fail_ttl" {
            return Ok(Validated {
                value: "0s".to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's column-tracking switch is retained as a deprecated global
        // compatibility variable but is now unconditionally ON. Normalize it
        // here so the shared table has the same observable value as Go's
        // custom GetGlobal hook after either boolean spelling.
        if self.name == "tidb_enable_column_tracking" {
            return Ok(Validated {
                value: "ON".to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_partition_prune_mode` closure upgrades the out-of-date
        // enum spellings (`static-only`/`dynamic-only`) before storing them.
        // The type metadata admits those spellings, but readers and the
        // session setter observe only the upgraded `static`/`dynamic` mode.
        if self.name == "tidb_partition_prune_mode" {
            let mode = crate::session_vars::PartitionPruneMode::from_str_value(&validated.value);
            let updated = mode.update();
            if !mode.valid() {
                return Err(ValidationError::WrongValue);
            }
            return Ok(Validated {
                value: updated.as_str().to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_enable_shared_lock_upgrade` gate is unavailable on the
        // classic kernel. Its Validation closure sees the normalized ON
        // value (including a typed `1`) and refuses it with
        // ErrWrongValueForVar; OFF remains the ordinary boolean default.
        if self.name == tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_UPGRADE
            && !tidb_config::kerneltype::is_next_gen()
            && validated.value == "ON"
        {
            return Err(ValidationError::WrongValue);
        }
        // Go's `tidb_enable_noop_functions` Validation refuses OFF while a
        // same-scope read-only/no-op variable remains ON. The lookup is
        // supplied by the session or GLOBAL writer; direct registry callers
        // fall back to each variable's declared default.
        if self.name == tidb_vardef::tidb_vars::TIDB_ENABLE_NOOP_FUNCS
            && validated.value == "OFF"
        {
            for incompatible in [
                "tx_read_only",
                "transaction_read_only",
                "offline_mode",
                "super_read_only",
                "read_only",
                "sql_auto_is_null",
            ] {
                let current = lookup
                    .and_then(|resolve| resolve(incompatible))
                    .or_else(|| get_sys_var(incompatible).map(|def| def.value.to_owned()));
                if current
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case("ON") || value == "1")
                {
                    return Err(ValidationError::SqlError(SqlError::new_f(
                        tidb_error::mysql::errcode::ErrNotSupportedYet,
                        "%s = OFF is not supported when %s = ON",
                        &[],
                        &[
                            FormatArg::from(self.name),
                            FormatArg::from(incompatible),
                        ],
                    )));
                }
            }
        }
        // Go's nextgen-only `TestTiDBPessimisticTransactionFairLocking`
        // exercises the variable-specific Validation closure: ON is rejected
        // with ErrNotSupportedInNextGen (1235) and the normalized fallback is
        // OFF. Classic builds retain the ordinary boolean behavior.
        if self.name == tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING
            && tidb_config::kerneltype::is_next_gen()
            && validated.value == "ON"
        {
            return Err(ValidationError::SqlError(SqlError::new_f(
                tidb_error::mysql::errcode::ErrNotSupportedYet,
                "%s is not supported in the next generation of TiDB",
                &[],
                &[FormatArg::from(self.name)],
            )));
        }
        // Go's nextgen-only `TestTiDBDMLTypeInNextGen` keeps STANDARD
        // available but rejects BULK with the same 1235 compatibility error.
        if self.name == tidb_vardef::tidb_vars::TIDB_DML_TYPE
            && tidb_config::kerneltype::is_next_gen()
            && validated.value.eq_ignore_ascii_case("bulk")
        {
            return Err(ValidationError::SqlError(SqlError::new_f(
                tidb_error::mysql::errcode::ErrNotSupportedYet,
                "%s is not supported in the next generation of TiDB",
                &[],
                &[FormatArg::from(self.name)],
            )));
        }
        // Go's nextgen-only replica-read validation retains LEADER and maps
        // every other non-empty mode back to LEADER after returning 1235.
        if self.name == tidb_vardef::tidb_vars::TIDB_REPLICA_READ
            && tidb_config::kerneltype::is_next_gen()
            && !validated.value.eq_ignore_ascii_case("leader")
            && !validated.value.is_empty()
        {
            return Err(ValidationError::SqlError(SqlError::new_f(
                tidb_error::mysql::errcode::ErrNotSupportedYet,
                "%s is not supported in the next generation of TiDB",
                &[],
                &[FormatArg::from(self.name)],
            )));
        }
        // Go's `tiflash_query_spill_ratio` keeps the generic float range
        // [0, 1], then narrows it in its variable-specific Validation closure
        // to [0, 0.85]. Values below zero have already been clamped to 0 by
        // ValidateFromType; values above 0.85 are refused with a bare 1105.
        if self.name == "tiflash_query_spill_ratio"
            && validated
                .value
                .parse::<f64>()
                .is_ok_and(|value| value > 0.85)
        {
            return Err(ValidationError::Refused(
                "The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85".into(),
            ));
        }
        // Go's `tidb_server_memory_limit` validation parses percentages and
        // binary-unit byte sizes, stores the parser's normalized spelling,
        // and clamps a positive limit below 512 MiB with a truncation warning.
        // The host total is a process fact, so a percentage is only resolved
        // when the runtime can provide it; byte forms remain deterministic.
        if self.name == "tidb_server_memory_limit" {
            let total = tidb_util::memory::mem_total().unwrap_or_default();
            let parsed = crate::varsutil::parse_memory_limit(total, &validated.value, original)
                .map_err(|_| {
                    ValidationError::SqlError(SqlError::new(
                        tidb_error::mysql::errcode::ErrTruncatedWrongValue,
                        &[FormatArg::from(self.name), FormatArg::from(original)],
                    ))
                })?;
            return Ok(Validated {
                value: parsed.normalized,
                truncated: parsed.clamped || validated.truncated,
            });
        }
        // Go's `tidb_server_memory_limit_sess_min_size` validation first
        // accepts an unsigned byte count, then falls back to its exact
        // integer binary-unit `parseByteSize` helper. Values in (0, 128) are
        // clamped with a truncation warning and the stored form is always the
        // decimal byte count, never the original suffix.
        if self.name == "tidb_server_memory_limit_sess_min_size" {
            let bytes = match validated.value.parse::<u64>() {
                Ok(bytes) => bytes,
                Err(_) => crate::varsutil::parse_byte_size(&validated.value)
                    .map(|(bytes, _)| bytes)
                    .ok_or(ValidationError::WrongType)?,
            };
            if bytes > 0 && bytes < 128 {
                return Ok(Validated {
                    value: "128".to_owned(),
                    truncated: true,
                });
            }
            return Ok(Validated {
                value: bytes.to_string(),
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_server_memory_limit_gc_trigger` accepts either a
        // decimal fraction or an integer percentage below 100, stores a
        // canonical fraction, and admits only the [0.51, 1] range. It also
        // refuses values below the current GOGC tuner threshold + 0.05. The
        // sibling lookup supplies the pending GLOBAL value during a table
        // write; direct registry validation uses Go's 0.6 default.
        if self.name == "tidb_server_memory_limit_gc_trigger" {
            let text = validated.value.trim();
            let fraction = if let Some(percent) = text.strip_suffix('%') {
                let percent = percent
                    .parse::<u64>()
                    .map_err(|_| ValidationError::WrongValue)?;
                if percent == 0 || percent >= 100 {
                    return Err(ValidationError::WrongValue);
                }
                percent as f64 / 100.0
            } else {
                text.parse::<f64>()
                    .map_err(|_| ValidationError::WrongValue)?
            };
            if !fraction.is_finite() || fraction < 0.51 || fraction > 1.0 {
                return Err(ValidationError::WrongValue);
            }
            let threshold = lookup
                .and_then(|read| read("tidb_gogc_tuner_threshold"))
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.6);
            if threshold != 0.0 && fraction < threshold + 0.05 {
                return Err(ValidationError::Refused(
                    "tidb_server_memory_limit_gc_trigger should be greater than tidb_gogc_tuner_threshold + 0.05"
                        .to_owned(),
                ));
            }
            return Ok(Validated {
                value: fraction.to_string(),
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_disable_txn_auto_retry` is retained only as a
        // compatibility alias: assigning OFF emits warning 1287 but stores
        // ON, because automatic retry can no longer be disabled.
        if self.name == "tidb_disable_txn_auto_retry" && validated.value == "OFF" {
            return Ok(Validated {
                value: "ON".to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's TTL schedule-window globals parse a short `HH:MM` value in
        // UTC, then store/display the full-day form with an explicit `+0000`
        // offset. Keep an already-expanded value canonical and reject invalid
        // clock text instead of silently storing it as an opaque TypeTime.
        if matches!(
            self.name,
            "tidb_ttl_job_schedule_window_start_time" | "tidb_ttl_job_schedule_window_end_time"
        ) {
            let text = validated.value.trim();
            let mut fields = text.split_whitespace();
            let parsed = fields.next().and_then(|clock| {
                let (hour, minute) = clock.split_once(':')?;
                Some((
                    hour.parse::<u8>().ok()?,
                    minute.parse::<u8>().ok()?,
                    fields.next(),
                ))
            });
            if let Some((hour, minute, offset)) = parsed {
                let valid_offset = offset.is_none_or(|offset| {
                    let bytes = offset.as_bytes();
                    if bytes.len() != 5 || !matches!(bytes[0], b'+' | b'-') {
                        return false;
                    }
                    let Ok(hours) = std::str::from_utf8(&bytes[1..3])
                        .unwrap_or_default()
                        .parse::<u8>()
                    else {
                        return false;
                    };
                    let Ok(minutes) = std::str::from_utf8(&bytes[3..5])
                        .unwrap_or_default()
                        .parse::<u8>()
                    else {
                        return false;
                    };
                    hours < 24 && minutes < 60
                });
                if fields.next().is_some() {
                    return Err(ValidationError::Refused(format!(
                        "invalid TTL job schedule window time: {original}"
                    )));
                }
                if hour < 24 && minute < 60 {
                    if valid_offset {
                        return Ok(Validated {
                            value: format!("{hour:02}:{minute:02} {}", offset.unwrap_or("+0000")),
                            truncated: validated.truncated,
                        });
                    }
                }
            }
            return Err(ValidationError::Refused(format!(
                "invalid TTL job schedule window time: {original}"
            )));
        }
        // Go's mutable collation validation (`sysvar.go`'s `checkCollation`)
        // resolves names through the parser registry, stores the canonical
        // spelling, and returns `ErrUnknownCollation` (1273) for a missing
        // entry.  The registry lookup is case-insensitive and also knows the
        // UTF8MB3 aliases, matching `collate.GetCollationByName`.
        // Go routes connection, server, and database collations through
        // `checkCollation` (`varsutil.go:57`): resolve the name case-insensitively,
        // store the canonical spelling, refuse a miss with 1273.
        if matches!(
            self.name,
            "collation_connection" | "collation_database" | "collation_server"
        ) {
            let collation =
                tidb_datatype::get_collation_by_name(&validated.value).map_err(|_| {
                    ValidationError::SqlError(SqlError::new(
                        tidb_error::mysql::errcode::ErrUnknownCollation,
                        &[FormatArg::from(original)],
                    ))
                })?;
            return Ok(Validated {
                value: collation.name,
                truncated: validated.truncated,
            });
        }
        // Go's `default_collation_for_utf8mb4` validation first resolves the
        // name through the same registry, then admits only the three default
        // utf8mb4 collations retained for compatibility.  The latter refusal
        // is the registered TiDB error 3721 rather than a generic variable
        // error, so preserve its catalogued message and code here.
        if self.name == "default_collation_for_utf8mb4" {
            let collation =
                tidb_datatype::get_collation_by_name(&validated.value).map_err(|_| {
                    ValidationError::SqlError(SqlError::new(
                        tidb_error::mysql::errcode::ErrUnknownCollation,
                        &[FormatArg::from(original)],
                    ))
                })?;
            if !matches!(
                collation.name.as_str(),
                "utf8mb4_bin" | "utf8mb4_general_ci" | "utf8mb4_0900_ai_ci"
            ) {
                return Err(ValidationError::SqlError(SqlError::new_f(
                    tidb_error::tidb::errcode::ErrInvalidDefaultUTF8MB4Collation,
                    tidb_error::tidb::errname::ErrInvalidDefaultUTF8MB4Collation.raw,
                    &[],
                    &[FormatArg::from(collation.name.as_str())],
                )));
            }
            return Ok(Validated {
                value: collation.name,
                truncated: validated.truncated,
            });
        }
        // Go keeps this compatibility variable in the integer range [1, 2],
        // then its variable-specific validation closure rejects 1 because the
        // planner no longer has an Analyze v1 path.
        if self.name == tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION && validated.value == "1" {
            return Err(ValidationError::Refused(
                "tidb_analyze_version=1 is no longer supported, please set tidb_analyze_version to 2"
                    .to_owned(),
            ));
        }
        // Go's `tidb_auto_analyze_ratio` accepts values above one but refuses
        // ratios below 0.00001 (within the source's 1e-9 tolerance), keeping
        // the previous setting intact with a bare validation error.
        if self.name == tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO {
            let ratio = validated
                .value
                .parse::<f64>()
                .map_err(|_| ValidationError::WrongType)?;
            const MIN_RATIO: f64 = 0.00001;
            const TOLERANCE: f64 = 1e-9;
            if ratio < MIN_RATIO && (ratio - MIN_RATIO).abs() > TOLERANCE {
                return Err(ValidationError::Refused(format!(
                    "the value of {} should be greater than or equal to {MIN_RATIO:.6}",
                    self.name
                )));
            }
        }
        // Go's `tidb_analyze_column_options` Validation accepts only ALL and
        // PREDICATE (case-insensitive); its GLOBAL setter uppercases the
        // accepted text before publication. Keep the stored value canonical
        // and refuse unknown options with the source's bare 1105 error.
        if self.name == tidb_vardef::tidb_vars::TIDB_ANALYZE_COLUMN_OPTIONS {
            let choice = validated.value.to_ascii_uppercase();
            if matches!(choice.as_str(), "ALL" | "PREDICATE") {
                return Ok(Validated {
                    value: choice,
                    truncated: validated.truncated,
                });
            }
            return Err(ValidationError::Refused(format!(
                "invalid value for tidb_analyze_column_options, it should be either 'ALL' or 'PREDICATE'"
            )));
        }
        // Direct registry users (including Go's validation unit tests) may
        // toggle the process-wide scheduler products without a GLOBAL table.
        // Preserve the source closure's prerequisite check here; SQL writes
        // additionally validate their pending GLOBAL image in `GlobalSysvars`.
        if self.name == tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY
            && (!tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst)
                || !tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
                    .load(std::sync::atomic::Ordering::SeqCst))
        {
            let run_auto_analyze =
                tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst);
            let enable_auto_analyze_priority_queue =
                tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
                    .load(std::sync::atomic::Ordering::SeqCst);
            return Err(ValidationError::Refused(format!(
                "cannot set {}: requires both tidb_enable_auto_analyze and tidb_enable_auto_analyze_priority_queue to be true. Current values: tidb_enable_auto_analyze={}, tidb_enable_auto_analyze_priority_queue={}",
                self.name, run_auto_analyze, enable_auto_analyze_priority_queue
            )));
        }
        // Pinned Go deprecated the auto-analyze scheduler switch after making
        // the priority queue unconditional. Its bool type validation still
        // runs first, then the variable-specific closure refuses OFF.
        if self.name == tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
            && validated.value == "OFF"
        {
            return Err(ValidationError::Refused(
                "tidb_enable_auto_analyze_priority_queue has been deprecated and TiDB will always use priority queue to schedule auto analyze"
                .to_owned(),
            ));
        }
        if self.name == tidb_vardef::tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE {
            let value = crate::embedding::normalize_openai_embedding_api_base(original)
                .map_err(ValidationError::Refused)?;
            return Ok(Validated {
                value,
                truncated: validated.truncated,
            });
        }
        if self.name == tidb_vardef::tidb_vars::TIDB_TXN_FILE_MIN_MUTATION_SIZE {
            let value = original.parse::<u64>().unwrap_or_default();
            const MIN_MUTATION_SIZE: u64 = 1 << 20;
            if value > 0 && value < MIN_MUTATION_SIZE {
                return Err(ValidationError::WrongValue);
            }
        }
        if self.name == "tidb_workload_repository_dest" {
            return tidb_workloadrepo::validate_dest(&validated.value)
                .map(|value| Validated {
                    value,
                    truncated: validated.truncated,
                })
                .map_err(ValidationError::Refused);
        }
        // Go's `tidb_isolation_read_engines` and
        // `tidb_mview_maintain_isolation_read_engines` validations share Go
        // master's `normalizeIsolationReadEnginesValue` helper: comma-split
        // engines trim, canonicalize case-insensitively to tikv/tiflash/tidb,
        // and an empty or unknown engine is `ErrWrongValueForVar`.
        if self.name == tidb_vardef::tidb_vars::TIDB_ISOLATION_READ_ENGINES
            || self.name == tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES
        {
            return normalize_isolation_read_engines_value(&validated.value)
                .map(|value| Validated {
                    value,
                    truncated: validated.truncated,
                })
                .map_err(|()| ValidationError::WrongValue);
        }
        // Go's `tidb_allow_fallback_to_tikv` closure accepts an empty list or
        // a comma-separated list containing only TiFlash, trims each token,
        // deduplicates it, and rejects every other engine.
        if self.name == tidb_vardef::tidb_vars::TIDB_ALLOW_FALLBACK_TO_TIKV {
            if validated.value.is_empty() {
                return Ok(validated);
            }
            let mut normalized = String::new();
            let mut seen_tiflash = false;
            for token in validated.value.split(',') {
                let token = token.trim();
                if token.is_empty() {
                    return Err(ValidationError::WrongValue);
                }
                if !token.eq_ignore_ascii_case("tiflash") {
                    return Err(ValidationError::WrongValue);
                }
                if !seen_tiflash {
                    normalized.push_str("tiflash");
                    seen_tiflash = true;
                }
            }
            return Ok(Validated {
                value: normalized,
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_scatter_region` Validation lowercases the requested
        // mode and admits only the empty (off), `table`, and `global` forms.
        // The registry's TypeStr normalization otherwise accepts any text,
        // so keep the source closure here rather than relying on metadata's
        // possible-values list (which is descriptive only).
        if self.name == tidb_vardef::tidb_vars::TIDB_SCATTER_REGION {
            let lowered = validated.value.to_ascii_lowercase();
            if matches!(lowered.as_str(), "" | "table" | "global") {
                return Ok(Validated {
                    value: lowered,
                    truncated: validated.truncated,
                });
            }
            return Err(ValidationError::Refused(format!(
                "invalid value for '{}', it should be either '', 'table' or 'global'",
                lowered
            )));
        }
        // Go's `tidb_mview_maintain_mem_quota` validation: a positive value
        // below 128 is clamped to 128 with Go's `ErrTruncatedWrongValue`
        // warning riding the SET.
        if self.name == tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA {
            if let Ok(int_val) = validated.value.parse::<i64>() {
                if (0..128).contains(&int_val) {
                    return Ok(Validated {
                        value: "128".to_owned(),
                        truncated: true,
                    });
                }
            }
        }
        // Go's `tidb_mview_maintain_import_disk_quota` validation: empty is
        // accepted; anything that is not a positive go-units size is
        // `ErrWrongValueForVar`.
        if self.name == tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA
            && !validated.value.is_empty()
        {
            let ok = tidb_config::configtypes::ram_in_bytes(&validated.value)
                .map(|bytes| bytes > 0)
                .unwrap_or(false);
            if !ok {
                return Err(ValidationError::WrongValue);
            }
        }
        // Go's `tidb_enable_table_partition` validation: partitioning is
        // ALWAYS on, so the closure returns `vardef.On` whatever was assigned
        // and only warns when the assignment was `OFF`. The stored value is
        // what `SHOW VARIABLES` reports, so `SET ... = off` followed by `SHOW
        // VARIABLES LIKE 'tidb_enable_table_partition'` reads `ON` (captured
        // through `gorun`, session and global scope alike).
        if self.name == "tidb_enable_table_partition" {
            return Ok(Validated {
                value: "ON".to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's deprecated planner/DDL compatibility switches are now always
        // enabled. Their Validation closures return `ON` for every boolean
        // spelling; the matching warning is appended by the session write
        // path when the caller explicitly requests `OFF`.
        if matches!(
            self.name,
            "tidb_enable_exchange_partition"
                | "tidb_enable_new_cost_interface"
                | "tidb_enable_tiflash_read_for_write_stmt"
        ) {
            return Ok(Validated {
                value: "ON".to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_enable_list_partition` validation: list partitioning is
        // also always on, but this one REFUSES anything that is not, with an
        // `errors.Errorf` whose text is the whole error. Captured:
        // `set tidb_enable_list_partition=off` -> `Error 1105 (HY000):
        // tidb_enable_list_partition is now always on, and cannot be turned
        // off`, and the variable stays `ON`.
        if self.name == "tidb_enable_list_partition" && validated.value != "ON" {
            return Err(ValidationError::Refused(
                "tidb_enable_list_partition is now always on, and cannot be turned off".into(),
            ));
        }
        // Go's TiDB/TiFlash hash-join-version validation closures accept the
        // two source values case-insensitively, retain the user's spelling,
        // and raise a bare error (1105) for every other value.
        if matches!(
            self.name,
            "tidb_hash_join_version" | "tiflash_hash_join_version"
        ) {
            use tidb_vardef::defaults::{HASH_JOIN_VERSION_LEGACY, HASH_JOIN_VERSION_OPTIMIZED};

            let is_valid = validated
                .value
                .eq_ignore_ascii_case(HASH_JOIN_VERSION_LEGACY)
                || validated
                    .value
                    .eq_ignore_ascii_case(HASH_JOIN_VERSION_OPTIMIZED);
            if !is_valid {
                return Err(ValidationError::Refused(format!(
                    "incorrect value: `{original}`. {} options: {HASH_JOIN_VERSION_LEGACY}, {HASH_JOIN_VERSION_OPTIMIZED}",
                    self.name
                )));
            }
        }
        // Go validates the original `tidb_service_scope` spelling through
        // `pkg/util/naming.Check`, then its SetGlobal hook stores lowercase.
        if self.name == "tidb_service_scope" {
            tidb_naming::check(original)
                .map_err(|error| ValidationError::Refused(error.to_string()))?;
            return Ok(Validated {
                value: validated.value.to_ascii_lowercase(),
                truncated: validated.truncated,
            });
        }
        // Go's `tidb_session_alias` validation: the alias is cut to 64 RUNES
        // (not bytes -- a 65-character Chinese alias loses exactly its last
        // character), and then, since it labels log lines as an identifier,
        // `util.IsInCorrectIdentifierName` strips every TRAILING SPACE off
        // what is left. Both cuts raise Go's `ErrTruncatedWrongValue`.
        // Captured: `set @@tidb_session_alias='abc  '` reads back as `abc` on
        // both `SELECT @@` and `SHOW VARIABLES`.
        if self.name == "tidb_session_alias" {
            let cut: String = validated.value.chars().take(64).collect();
            let trimmed = cut.trim_end_matches(' ');
            if trimmed.len() != validated.value.len() {
                return Ok(Validated {
                    value: trimmed.to_owned(),
                    truncated: true,
                });
            }
            return Ok(validated);
        }
        // Go's `max_allowed_packet` validation (`sysvar.go:2193`): the
        // accepted value is truncated DOWN to a multiple of 1024, and the
        // rounding is reported as `ErrTruncatedWrongValue` (1292) -- which is
        // what the `truncated` flag carries to the statement.
        //
        // `MinValue` is 1024, so the type check has already refused anything
        // below one full multiple; the remainder can never take the value to
        // zero here.
        //
        // NOT MODELLED (needs a coded error this tier's `VarErrorKind` does
        // not carry): Go additionally REFUSES `SET SESSION max_allowed_packet`
        // with `ErrReadOnly` (1621, "SESSION variable 'max_allowed_packet' is
        // read-only. Use SET GLOBAL to assign the value"), and refuses
        // `SET GLOBAL` in starter deployments.
        if self.name == "max_allowed_packet" {
            if let Ok(parsed) = validated.value.parse::<u64>() {
                let remainder = parsed % 1024;
                if remainder != 0 {
                    return Ok(Validated {
                        value: (parsed - remainder).to_string(),
                        truncated: true,
                    });
                }
            }
            return Ok(validated);
        }
        // Go's `checkCharacterSet` (`varsutil.go:76`) covers every mutable
        // character-set variable. An empty `character_set_results` is the
        // sole exception (it means no result conversion); all other empty
        // values are `ErrWrongValueForVar` (1231) with NULL. Unknown names
        // are `ErrUnknownCharacterSet` (1115), and a hit stores the
        // canonical charset name.
        if matches!(
            self.name,
            "character_set_client"
                | "character_set_connection"
                | "character_set_database"
                | "character_set_results"
                | "character_set_server"
        ) {
            if self.name == "character_set_results" && validated.value.is_empty() {
                return Ok(validated);
            }
            if validated.value.is_empty() {
                return Err(ValidationError::WrongValue);
            }
            let charset = tidb_datatype::get_charset_info(&validated.value).map_err(|_| {
                ValidationError::SqlError(SqlError::new(
                    tidb_error::mysql::errcode::ErrUnknownCharacterSet,
                    &[FormatArg::from(original)],
                ))
            })?;
            return Ok(Validated {
                value: charset.name,
                truncated: validated.truncated,
            });
        }
        // Go's mpp_exchange_compression_mode Validation (`sysvar.go:3308`):
        // the mode name must parse (`ToExchangeCompressionMode`); a miss is
        // refused with the option list spelled out in the message (a bare
        // errors.Errorf, so it reports as 1105).
        if self.name == tidb_vardef::tidb_vars::MPP_EXCHANGE_COMPRESSION_MODE {
            if tidb_vardef::modes::to_exchange_compression_mode(&validated.value).is_none() {
                return Err(ValidationError::Refused(format!(
                    "incorrect value: `{original}`. mpp_exchange_compression_mode options: NONE, FAST, HIGH_COMPRESSION, UNSPECIFIED",
                    original = original
                )));
            }
            return Ok(validated);
        }
        // Go's tidb_runtime_filter_type Validation (`sysvar.go:3726`): a
        // comma-separated list whose tokens must each be IN or MIN_MAX in
        // any case; a bad token is refused with the hint message (a bare
        // errors.New, so it reports as 1105).
        if self.name == tidb_vardef::tidb_vars::TIDB_RUNTIME_FILTER_TYPE_NAME {
            let ok = validated.value.split(',').all(|token| {
                matches!(token.trim().to_ascii_uppercase().as_str(), "IN" | "MIN_MAX")
            });
            if !ok {
                return Err(ValidationError::Refused(format!(
                    "incorrect value: {original}. tidb_runtime_filter_type should be sepreated by , such as IN, also we only support IN and MIN_MAX now. "
                )));
            }
            return Ok(validated);
        }
        // Go's tidb_runtime_filter_mode Validation (`sysvar.go:3741`): only
        // the exact spellings OFF and LOCAL pass (the lookup is
        // case-sensitive); the refusal is the option-list message, 1105.
        if self.name == tidb_vardef::tidb_vars::TIDB_RUNTIME_FILTER_MODE_NAME {
            if !matches!(validated.value.as_str(), "OFF" | "LOCAL") {
                return Err(ValidationError::Refused(format!(
                    "incorrect value: {original}. tidb_runtime_filter_mode options: OFF "
                )));
            }
            return Ok(validated);
        }
        // Go's init_connect Validation (`sysvar.go:704`): the value must
        // parse as SQL (Go parses with the session's SQL mode; this
        // boundary has no mode, so the stock mode stands in). A parse
        // failure is `ErrWrongTypeForVar` (1232); an empty value parses as
        // zero statements and passes, as in Go.
        if self.name == "init_connect" {
            if !validated.value.is_empty() && tidb_parser::parse(&validated.value).is_err() {
                return Err(ValidationError::WrongType);
            }
            return Ok(validated);
        }
        // Go's mpp_version Validation (`sysvar.go:3335`): the value must
        // parse (`ToMppVersion`); a miss is refused with the option list
        // `-1 (unspecified), 0, 1, 2, 3` (a bare errors.Errorf, so it
        // reports as 1105).
        if self.name == tidb_vardef::tidb_vars::MPP_VERSION {
            if tidb_vardef::modes::to_mpp_version(&validated.value).is_none() {
                return Err(ValidationError::Refused(format!(
                    "incorrect value: `{original}`. mpp_version options: -1 (unspecified), 0, 1, 2, 3",
                )));
            }
            return Ok(validated);
        }
        // The mem-arbitrator Validations (`sysvar.go`, the
        // tidb_mem_arbitrator_* entries): mode lowercases and whitelists
        // disable/standard/priority; wait_averse accepts exactly "0", "1"
        // and "nolimit"; query_reserved accepts "0" or any integer > 1;
        // soft_limit accepts the disable sentinel, `auto`, a positive ratio
        // through 1.0, or an integer byte count greater than 1. All
        // refusals are bare errors, 1105.
        if self.name == "tidb_mem_arbitrator_mode" {
            let lowered = validated.value.to_ascii_lowercase();
            if matches!(lowered.as_str(), "disable" | "standard" | "priority") {
                return Ok(Validated {
                    value: lowered,
                    truncated: validated.truncated,
                });
            }
            return Err(ValidationError::Refused(format!(
                "incorrect value: {original}. tidb_mem_arbitrator_mode options: disable, standard, priority"
            )));
        }
        if self.name == "tidb_mem_arbitrator_wait_averse" {
            if matches!(validated.value.as_str(), "0" | "1" | "nolimit") {
                return Ok(validated);
            }
            return Err(ValidationError::Refused(
                "invalid tidb_mem_arbitrator_wait_averse value; only 0, 1 and nolimit are accepted"
                    .to_owned(),
            ));
        }
        if self.name == "tidb_mem_arbitrator_query_reserved" {
            if validated.value == "0" {
                return Ok(validated);
            }
            if let Ok(v) = validated.value.parse::<u64>() {
                // Go parses uint64 and then compares the int64 conversion;
                // values above MaxInt64 wrap negative and are refused.
                if v > 1 && v <= i64::MAX as u64 {
                    return Ok(validated);
                }
            }
            return Err(ValidationError::Refused(
                "invalid tidb_mem_arbitrator_query_reserved value".to_owned(),
            ));
        }
        if self.name == "tidb_mem_arbitrator_soft_limit" && !validated.value.is_empty() {
            if validated.value == "0" {
                return Ok(validated);
            }
            if validated.value.eq_ignore_ascii_case("auto") {
                return Ok(Validated {
                    value: "auto".to_owned(),
                    truncated: validated.truncated,
                });
            }
            if let Ok(integer) = validated.value.parse::<i64>() {
                if integer > 1 {
                    return Ok(validated);
                }
                if integer <= 0 {
                    return Err(ValidationError::Refused(
                        "tidb_mem_arbitrator_soft_limit: 0 (default); (0, 1.0] float-rate * server-limit; (1, server-limit] integer bytes; auto;"
                            .to_owned(),
                    ));
                }
            }
            if let Ok(ratio) = validated.value.parse::<f64>() {
                if ratio > 0.0 && ratio <= 1.0 {
                    return Ok(validated);
                }
            }
            return Err(ValidationError::Refused(
                "tidb_mem_arbitrator_soft_limit: 0 (default); (0, 1.0] float-rate * server-limit; (1, server-limit] integer bytes; auto;"
                    .to_owned(),
            ));
        }
        // Go's tidb_opt_index_join_build_v2 (`sysvar.go:2874`): the
        // planner always uses the v2 path, so a falsy set is refused with
        // the always-enabled message (bare error, 1105) and a truthy set
        // normalizes to ON.
        if self.name == "tidb_opt_index_join_build_v2" {
            let on = matches!(
                validated.value.to_ascii_lowercase().as_str(),
                "on" | "1" | "true"
            );
            if !on {
                return Err(ValidationError::Refused(
                    "tidb_opt_index_join_build_v2 is now always enabled and cannot be turned off"
                        .to_owned(),
                ));
            }
            return Ok(Validated {
                value: "ON".to_owned(),
                truncated: validated.truncated,
            });
        }
        // Go's tidb_schema_cache_size Validation (`sysvar.go:3772` via
        // `parseSchemaCacheSize`, `varsutil.go:537`): a byte-size string or
        // plain integer passes through its parsed spelling, sizes below the
        // 64MB floor clamp up to "64MB", sizes above i64::MAX clamp down,
        // and anything unparseable is ErrTruncatedWrongValue (1292). Go
        // additionally appends a 1365 warning on both clamps; this
        // boundary has no warning sink, so only the value is adjusted.
        if self.name == "tidb_schema_cache_size" {
            const LOWER_BOUND: u64 = 64 << 20;
            if let Some((bytes, spelled)) = crate::varsutil::parse_byte_size(&validated.value) {
                if bytes > 0 && bytes < LOWER_BOUND {
                    return Ok(Validated {
                        value: "64MB".to_owned(),
                        truncated: true,
                    });
                }
                if bytes > i64::MAX as u64 {
                    return Ok(Validated {
                        value: (i64::MAX as u64).to_string(),
                        truncated: true,
                    });
                }
                if spelled != validated.value {
                    return Ok(Validated {
                        value: spelled,
                        truncated: validated.truncated,
                    });
                }
                return Ok(validated);
            }
            return Err(ValidationError::SqlError(SqlError::new(
                tidb_error::mysql::errcode::ErrTruncatedWrongValue,
                &[
                    FormatArg::from("tidb_schema_cache_size"),
                    FormatArg::from(original),
                ],
            )));
        }
        // Go's `ValidAnalyzeSkipColumnTypes` (`varsutil.go:501`): tokens are
        // lowercased and trimmed, must each be one of the seven column
        // types, and the joined lower-case list is stored; anything else is
        // `ErrWrongValueForVar` (1231) carrying the original value.
        if self.name == "tidb_analyze_skip_column_types" {
            const ALLOWED: [&str; 7] = [
                "json",
                "text",
                "mediumtext",
                "longtext",
                "blob",
                "mediumblob",
                "longblob",
            ];
            if validated.value.is_empty() {
                return Ok(Validated {
                    value: String::new(),
                    truncated: validated.truncated,
                });
            }
            let mut column_types: Vec<String> = Vec::new();
            for item in validated.value.split(',') {
                let column_type = item.trim().to_ascii_lowercase();
                if !ALLOWED.contains(&column_type.as_str()) {
                    return Err(ValidationError::WrongValue);
                }
                column_types.push(column_type);
            }
            return Ok(Validated {
                value: column_types.join(","),
                truncated: validated.truncated,
            });
        }
        // Go's tidb_max_dist_task_nodes Validation (`sysvar.go`): zero is
        // not a legal node count — the message names the legal domain
        // (-1 or [1, 128]); other in-range values pass (a bare
        // errors.New, so it reports as 1105).
        if self.name == tidb_vardef::tidb_vars::TIDB_MAX_DIST_TASK_NODES {
            if let Ok(nodes) = validated.value.parse::<i64>() {
                if nodes == 0 {
                    return Err(ValidationError::Refused(
                        "max_dist_task_nodes should be -1 or [1, 128]".to_owned(),
                    ));
                }
            }
            return Ok(validated);
        }
        // Go's `tiflash_hashagg_preaggregation_mode` Validation is a
        // case-sensitive lookup over the three TiFlash modes. Preserve the
        // user's accepted spelling and refuse a miss with its option-list
        // error (a bare 1105).
        if self.name == tidb_vardef::tidb_vars::TIFLASH_HASH_AGG_PRE_AGG_MODE {
            if matches!(
                validated.value.as_str(),
                "force_preagg" | "auto" | "force_streaming"
            ) {
                return Ok(validated);
            }
            return Err(ValidationError::Refused(format!(
                "incorrect value: `{original}`. tiflash_hashagg_preaggregation_mode options: force_preagg, auto, force_streaming"
            )));
        }
        // Go's tidb_evolve_plan_baselines Validation (`sysvar.go`): ON is
        // refused unless the test-only CheckTableBeforeDrop knob is set,
        // which is false in every deployment (a bare errors.Errorf, 1105).
        if self.name == tidb_vardef::tidb_vars::TIDB_EVOLVE_PLAN_BASELINES
            && validated.value == "ON"
        {
            return Err(ValidationError::Refused(
                "Cannot enable baseline evolution feature, it is not generally available now"
                    .to_owned(),
            ));
        }
        // Go's `checkIsolationLevel` (`varsutil.go:116`): SERIALIZABLE and
        // READ-UNCOMMITTED are refused with `ErrUnsupportedIsolationLevel`
        // (8048) unless the session's own
        // `tidb_skip_isolation_level_check` is ON, in which case the set
        // proceeds with that warning (no sink on this boundary).
        if self.name == "tx_isolation_one_shot"
            && matches!(
                validated.value.as_str(),
                "SERIALIZABLE" | "READ-UNCOMMITTED"
            )
        {
            let skip_on = lookup
                .and_then(|read| read("tidb_skip_isolation_level_check"))
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "on" | "1" | "true"))
                .unwrap_or(false);
            if !skip_on {
                return Err(ValidationError::SqlError(SqlError::new(
                    8048,
                    &[FormatArg::from(validated.value.as_str())],
                )));
            }
            return Ok(validated);
        }
        // Go's `validateReadConsistencyLevel` (`session.go:702`): only
        // `strict` and `weak` in any case pass, stored as typed; everything
        // else is `ErrWrongTypeForVar` (1232).
        if self.name == "tidb_read_consistency" {
            let lowered = validated.value.to_ascii_lowercase();
            if lowered != "strict" && lowered != "weak" {
                return Err(ValidationError::WrongType);
            }
            return Ok(validated);
        }
        if self.name != "sql_mode" {
            return Ok(validated);
        }
        // Go: `normalizedValue = mysql.FormatSQLModeStr(normalizedValue)`
        // then `mysql.GetSQLMode(normalizedValue)`. The formatting uppercases,
        // expands the combination modes (TRADITIONAL, ANSI, ORACLE, ...) into
        // their member modes while KEEPING the combination name itself, and
        // drops duplicates; parsing then rejects any token that names no mode
        // -- which is also what refuses a numeric bitmask, since MySQL's
        // numeric sql_mode form is not accepted here.
        let formatted = tidb_mysql::format_sql_mode_str(&validated.value);
        match tidb_mysql::get_sql_mode(&formatted) {
            Ok(_) => Ok(Validated {
                value: formatted,
                truncated: validated.truncated,
            }),
            Err(invalid) => Err(ValidationError::SqlError(invalid.sql_error)),
        }
    }

    /// Go's `value == "" && ((AllowEmpty && scope == ScopeSession) ||
    /// AllowEmptyAll)`: the empty string bypasses type validation entirely.
    #[must_use]
    pub fn allows_empty_value(&self, scope: u8) -> bool {
        (scope == SCOPE_SESSION && ALLOW_EMPTY_VARS.contains(&self.name))
            || ALLOW_EMPTY_ALL_VARS.contains(&self.name)
    }

    /// Go `checkUInt64SystemVar`.
    fn check_uint64(&self, value: &str) -> Result<Validated, ValidationError> {
        if self.allow_auto_value && value == "-1" {
            return Ok(Validated {
                value: value.to_owned(),
                truncated: false,
            });
        }
        if value.is_empty() {
            return Err(ValidationError::WrongType);
        }
        if value.starts_with('-') {
            // A negative value parses as an integer, then clamps to the
            // minimum with a truncation warning.
            value
                .parse::<i64>()
                .map_err(|_| ValidationError::WrongType)?;
            return Ok(Validated {
                value: self.min_value.to_string(),
                truncated: true,
            });
        }
        let parsed: u64 = value.parse().map_err(|_| ValidationError::WrongType)?;
        if parsed < self.min_value as u64 {
            return Ok(Validated {
                value: self.min_value.to_string(),
                truncated: true,
            });
        }
        if parsed > self.max_value {
            return Ok(Validated {
                value: self.max_value.to_string(),
                truncated: true,
            });
        }
        Ok(Validated {
            value: value.to_owned(),
            truncated: false,
        })
    }

    /// Go `checkInt64SystemVar`.
    fn check_int64(&self, value: &str) -> Result<Validated, ValidationError> {
        if self.allow_auto_value && value == "-1" {
            return Ok(Validated {
                value: value.to_owned(),
                truncated: false,
            });
        }
        let parsed: i64 = value.parse().map_err(|_| ValidationError::WrongType)?;
        if parsed < self.min_value {
            return Ok(Validated {
                value: self.min_value.to_string(),
                truncated: true,
            });
        }
        if parsed > self.max_value as i64 {
            return Ok(Validated {
                value: self.max_value.to_string(),
                truncated: true,
            });
        }
        Ok(Validated {
            value: value.to_owned(),
            truncated: false,
        })
    }

    /// Go `checkBoolSystemVar`.
    fn check_bool(&self, value: &str) -> Result<Validated, ValidationError> {
        if value.eq_ignore_ascii_case("ON") {
            return Ok(Validated {
                value: "ON".to_owned(),
                truncated: false,
            });
        }
        if value.eq_ignore_ascii_case("OFF") {
            return Ok(Validated {
                value: "OFF".to_owned(),
                truncated: false,
            });
        }
        if let Ok(parsed) = value.parse::<i64>() {
            // Two conversion rules: the default accepts only 0 and 1, while a
            // subset converts any negative integer to ON.
            if self.auto_convert_negative_bool {
                if parsed == 1 || parsed < 0 {
                    return Ok(Validated {
                        value: "ON".to_owned(),
                        truncated: false,
                    });
                }
                if parsed == 0 {
                    return Ok(Validated {
                        value: "OFF".to_owned(),
                        truncated: false,
                    });
                }
            } else {
                if parsed == 0 {
                    return Ok(Validated {
                        value: "OFF".to_owned(),
                        truncated: false,
                    });
                }
                if parsed == 1 {
                    return Ok(Validated {
                        value: "ON".to_owned(),
                        truncated: false,
                    });
                }
            }
        }
        Err(ValidationError::WrongValue)
    }

    /// Go `checkEnumSystemVar`: the value may be a name or its ordinal
    /// position, so `0` selects the first possible value.
    fn check_enum(&self, value: &str) -> Result<Validated, ValidationError> {
        for (index, candidate) in self.possible_values.iter().enumerate() {
            if value.eq_ignore_ascii_case(candidate) || value == index.to_string() {
                return Ok(Validated {
                    value: (*candidate).to_owned(),
                    truncated: false,
                });
            }
        }
        Err(ValidationError::WrongValue)
    }

    /// Go `checkFloatSystemVar`.
    fn check_float(&self, value: &str) -> Result<Validated, ValidationError> {
        if value.is_empty() {
            return Err(ValidationError::WrongType);
        }
        let parsed: f64 = value.parse().map_err(|_| ValidationError::WrongType)?;
        if parsed < self.min_value as f64 {
            return Ok(Validated {
                value: self.min_value.to_string(),
                truncated: true,
            });
        }
        if parsed > self.max_value as f64 {
            return Ok(Validated {
                value: self.max_value.to_string(),
                truncated: true,
            });
        }
        Ok(Validated {
            value: value.to_owned(),
            truncated: false,
        })
    }

    /// Go `checkDurationSystemVar`: parse `time.Duration`, clamp to the
    /// configured nanosecond range, and render through `Duration.String()`.
    fn check_duration(&self, value: &str) -> Result<Validated, ValidationError> {
        let parsed = tidb_config::configtypes::parse_go_duration(value)
            .map_err(|_| ValidationError::WrongType)?;
        if parsed < self.min_value {
            return Ok(Validated {
                value: tidb_model::go_duration::format_go_duration(self.min_value),
                truncated: true,
            });
        }
        if parsed >= 0 && (parsed as u64) > self.max_value {
            return Ok(Validated {
                value: tidb_model::go_duration::format_go_duration(self.max_value as i64),
                truncated: true,
            });
        }
        Ok(Validated {
            value: tidb_model::go_duration::format_go_duration(parsed),
            truncated: false,
        })
    }
}

/// The wire flags Go `GetNativeValType` returns alongside the datum. Go
/// spells them as `mysql.UnsignedFlag | mysql.BinaryFlag` numeric masks; the
/// two facts are carried by name here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeValFlags {
    /// Go `mysql.UnsignedFlag`.
    pub unsigned: bool,
    /// Go `mysql.BinaryFlag`.
    pub binary: bool,
}

impl SysVarDef {
    /// Go `SysVar.GetNativeValType`: the datum domain a `@@var` read reports,
    /// decided by the registry's `Type`, never the variable's name.
    ///
    /// `TypeUnsigned` parses the stored string as u64 (unparsable becomes 0)
    /// and reports `LONGLONG UNSIGNED BINARY`; `TypeBool` reports the signed
    /// `1`/`0` of `TiDBOptOn` as `LONGLONG BINARY`; everything else stays the
    /// stored string as `VAR_STRING` with no flags.
    #[must_use]
    pub fn native_val_type(
        &self,
        val: &str,
    ) -> (
        tidb_datatype::Datum,
        tidb_datatype::FieldTypeCode,
        NativeValFlags,
    ) {
        use tidb_datatype::{Datum, FieldTypeCode};
        match self.var_type {
            VarType::Unsigned => (
                Datum::UInt(val.parse::<u64>().unwrap_or(0)),
                FieldTypeCode::LongLong,
                NativeValFlags {
                    unsigned: true,
                    binary: true,
                },
            ),
            VarType::Bool => {
                // Go `TiDBOptOn`: "ON" case-folded, or exactly "1".
                let on = val.eq_ignore_ascii_case("ON") || val == "1";
                (
                    Datum::Int(i64::from(on)),
                    FieldTypeCode::LongLong,
                    NativeValFlags {
                        unsigned: false,
                        binary: true,
                    },
                )
            }
            _ => (
                Datum::new_string(val.as_bytes().to_vec()),
                FieldTypeCode::VarString,
                NativeValFlags::default(),
            ),
        }
    }

    /// Go `SysVar.SkipSysvarCache`: the GC variables and the external
    /// timestamp live in TiKV-backed tables, so peers must not re-execute
    /// them through the sysvar cache.
    #[must_use]
    pub fn skip_sysvar_cache(&self) -> bool {
        matches!(
            self.name,
            tidb_vardef::tidb_vars::TIDB_GC_ENABLE
                | tidb_vardef::tidb_vars::TIDB_GC_RUN_INTERVAL
                | tidb_vardef::tidb_vars::TIDB_GC_LIFETIME
                | tidb_vardef::tidb_vars::TIDB_GC_CONCURRENCY
                | tidb_vardef::tidb_vars::TIDB_GC_SCAN_LOCK_MODE
                | tidb_vardef::tidb_vars::TIDB_EXTERNAL_TS
        )
    }
}

/// The variables `sysvar.go` marks `Depended: true` — other variables refuse
/// to load correctly until these are set, so session-state decode orders them
/// first. The generated catalog does not carry the flag, so the source's four
/// carriers are pinned here.
pub const DEPENDED_SYSVARS: [&str; 4] = [
    tidb_vardef::tidb_vars::TIDB_ENABLE_LOCAL_TXN,
    tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS,
    tidb_vardef::tidb_vars::TIDB_ALLOW_MPP_EXECUTION,
    tidb_vardef::tidb_vars::TIDB_ENABLE_NOOP_FUNCS,
];

/// Go `OrderByDependency`: the depended variables move to the front; unknown
/// names count as not depended. Go iterates a map, so order within each group
/// is unspecified; input order is preserved here, a determinism refinement.
#[must_use]
pub fn order_by_dependency<S: AsRef<str>>(names: &[S]) -> Vec<String> {
    let mut depended = Vec::new();
    let mut not_depended = Vec::new();
    for name in names {
        let name = name.as_ref();
        let is_depended = get_sys_var(name).is_some() && DEPENDED_SYSVARS.contains(&name);
        if is_depended {
            depended.push(name.to_owned());
        } else {
            not_depended.push(name.to_owned());
        }
    }
    depended.extend(not_depended);
    depended
}

#[cfg(test)]
mod variable_core_tests {
    use super::*;
    use tidb_datatype::{Datum, FieldTypeCode};

    fn sysvar_of_type(var_type: VarType) -> SysVarDef {
        SysVarDef {
            var_type,
            ..SysVarDef::PLACEHOLDER
        }
    }

    // Go `TestGetNativeValType`.
    #[test]
    fn native_val_types_follow_the_registry_type() {
        let boolean = sysvar_of_type(VarType::Bool);
        let (val, code, flags) = boolean.native_val_type("ON");
        assert_eq!(val, Datum::Int(1));
        assert_eq!(code, FieldTypeCode::LongLong);
        assert_eq!(
            flags,
            NativeValFlags {
                unsigned: false,
                binary: true
            }
        );
        assert_eq!(boolean.native_val_type("OFF").0, Datum::Int(0));
        assert_eq!(boolean.native_val_type("bogus").0, Datum::Int(0));
        assert_eq!(boolean.native_val_type("1").0, Datum::Int(1));

        let unsigned = sysvar_of_type(VarType::Unsigned);
        let (val, code, flags) = unsigned.native_val_type("1234");
        assert_eq!(val, Datum::UInt(1234));
        assert_eq!(code, FieldTypeCode::LongLong);
        assert_eq!(
            flags,
            NativeValFlags {
                unsigned: true,
                binary: true
            }
        );
        // Unparsable converts to zero.
        assert_eq!(unsigned.native_val_type("bogus").0, Datum::UInt(0));

        let string = sysvar_of_type(VarType::Str);
        let (val, code, flags) = string.native_val_type("1234");
        assert_eq!(val, Datum::new_string(b"1234".to_vec()));
        assert_eq!(code, FieldTypeCode::VarString);
        assert_eq!(flags, NativeValFlags::default());
    }

    // Go `TestSkipSysvarCache`.
    #[test]
    fn only_the_tikv_backed_variables_skip_the_cache() {
        for name in [
            "tidb_gc_enable",
            "tidb_gc_run_interval",
            "tidb_gc_life_time",
            "tidb_gc_concurrency",
            "tidb_gc_scan_lock_mode",
            "tidb_external_ts",
        ] {
            let sv = get_sys_var(name).unwrap_or_else(|| panic!("{name} in catalog"));
            assert!(sv.skip_sysvar_cache(), "{name}");
        }
        assert!(!get_sys_var("require_secure_transport")
            .unwrap()
            .skip_sysvar_cache());
        assert!(!get_sys_var("tidb_enable_async_commit")
            .unwrap()
            .skip_sysvar_cache());
    }

    // Go `TestOrderByDependency`: depended names come first, unknown names
    // survive as not depended.
    #[test]
    fn depended_variables_order_first() {
        let names = [
            "unknown",
            "tx_read_only",
            "sql_auto_is_null",
            "tidb_enable_noop_functions",
            "tidb_enforce_mpp",
            "tidb_allow_mpp",
            "tidb_enable_local_txn",
            "tidb_enable_plan_replayer_continuous_capture",
            "tidb_enable_historical_stats",
        ];
        let ordered = order_by_dependency(&names);
        let index = |name: &str| {
            ordered
                .iter()
                .position(|n| n == name)
                .unwrap_or_else(|| panic!("{name}"))
        };

        assert!(index("tx_read_only") > index("tidb_enable_noop_functions"));
        assert!(index("sql_auto_is_null") > index("tidb_enable_noop_functions"));
        assert!(index("tidb_enforce_mpp") > index("tidb_allow_mpp"));
        assert!(
            index("tidb_enable_plan_replayer_continuous_capture")
                > index("tidb_enable_historical_stats")
        );
        assert!(ordered.contains(&"unknown".to_owned()));
        assert_eq!(ordered.len(), names.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry must hold every entry Go's own `GetSysVars()` reported
    /// when this table was captured.
    #[test]
    fn the_registry_is_complete_and_sorted() {
        // Go `defaultSysVars` carries 495 explicit entries at master
        // `94a9cbedab` (490 before the five `tidb_mview_*` variables); the
        // Rust registry additionally carries the inherited MySQL/InnoDB
        // variables, hence 971 = the pre-mview 965 + Go's five mview entries
        // and the shared-lock-upgrade rollout switch.
        assert_eq!(SYS_VARS.len(), 971);
        for pair in SYS_VARS.windows(2) {
            assert!(
                pair[0].name < pair[1].name,
                "unsorted: {} then {}",
                pair[0].name,
                pair[1].name
            );
            assert_eq!(
                pair[0].name.to_ascii_lowercase(),
                pair[0].name,
                "Go stores registry names lowercased"
            );
        }
        // The lookup is case-insensitive, as Go's is after lowercasing.
        assert!(get_sys_var("AUTOCOMMIT").is_some());
        assert!(get_sys_var("no_such_variable").is_none());
    }

    /// Go `TestDefaultValuesAreSettable`: every writable default must pass its
    /// own type and variable validation in each scope it advertises, without
    /// changing the canonical registry spelling.
    #[test]
    fn default_values_are_settable_like_go() {
        for definition in SYS_VARS {
            if definition.has_session_scope()
                && !definition.is_read_only()
                && !definition.is_internal_session_variable()
            {
                let validated = definition
                    .validate_in_scope(definition.value, SCOPE_SESSION)
                    .unwrap_or_else(|error| {
                        panic!(
                            "session default rejected for {}: {error:?}",
                            definition.name
                        )
                    });
                assert_eq!(validated.value, definition.value, "{}", definition.name);
            }
            if definition.has_global_scope() && !definition.is_read_only() {
                let validated = definition
                    .validate_in_scope(definition.value, SCOPE_GLOBAL)
                    .unwrap_or_else(|error| {
                        panic!("global default rejected for {}: {error:?}", definition.name)
                    });
                assert_eq!(validated.value, definition.value, "{}", definition.name);
            }
        }
    }

    /// Go `TestSettersandGetters`: scope metadata must agree with the live
    /// session/global setter routes, including read-only and internal names.
    #[test]
    fn setter_scope_contract_matches_go() {
        let mut vars = crate::vars::SessionVars::new();
        for definition in SYS_VARS {
            if !definition.has_session_scope() {
                let error = vars
                    .set_system(definition.name, definition.value.to_owned())
                    .expect_err("SESSION must reject a non-session variable");
                assert!(
                    matches!(
                        error,
                        crate::vars::VarError::ReadOnlyVariable(_)
                            | crate::vars::VarError::GlobalOnlyVariable(_)
                            | crate::vars::VarError::UnknownSystemVariable(_)
                    ),
                    "unexpected SESSION error for {}: {error:?}",
                    definition.name
                );
            }
            if !definition.has_global_scope() && !definition.has_instance_scope() {
                let error = vars
                    .set_global(definition.name, definition.value.to_owned())
                    .expect_err("GLOBAL must reject a non-global/non-instance variable");
                assert!(
                    matches!(
                        error,
                        crate::vars::VarError::ReadOnlyVariable(_)
                            | crate::vars::VarError::SessionOnlyVariable(_)
                    ),
                    "unexpected GLOBAL error for {}: {error:?}",
                    definition.name
                );
            }
        }
    }

    /// Go `TestInstanceConfigHasMatchingSysvar`: every serialized `[instance]`
    /// option must have a same-named system variable for unified routing.
    #[test]
    fn instance_config_keys_have_matching_sysvars() {
        let json = tidb_config::config_tree::new_config()
            .get_json_config()
            .expect("default config serializes as JSON");
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("GetJSONConfig output is valid JSON");
        let instance = value
            .get("instance")
            .and_then(serde_json::Value::as_object)
            .expect("config includes an instance object");
        for name in instance.keys() {
            assert!(
                get_sys_var(name).is_some(),
                "config option instance.{name} requires a matching sysvar"
            );
        }
    }

    /// Transcreated from Go `sysvar_test.go` `TestSQLSelectLimit`: the
    /// out-of-range value converts rather than erroring.
    #[test]
    fn sql_select_limit() {
        let sv = get_sys_var("sql_select_limit").unwrap();
        assert_eq!(sv.validate("-10").unwrap().value, "0");
        assert_eq!(sv.validate("9999").unwrap().value, "9999");
    }

    /// Transcreated from Go `TestMaxExecutionTime`.
    #[test]
    fn max_execution_time() {
        let sv = get_sys_var("max_execution_time").unwrap();
        assert_eq!(sv.validate("-10").unwrap().value, "0");
        assert_eq!(sv.validate("99999").unwrap().value, "99999");
    }

    /// Transcreated from Go `TestNetBufferLength`: unsigned bounds clamp to
    /// 1024 and 1048576, while an in-range value is preserved.
    #[test]
    fn net_buffer_length_validation_matches_go() {
        let sv = get_sys_var("net_buffer_length").unwrap();
        assert_eq!(sv.validate("1").unwrap().value, "1024");
        assert_eq!(sv.validate("10485760").unwrap().value, "1048576");
        assert_eq!(sv.validate("524288").unwrap().value, "524288");
    }

    /// Transcreated from Go `TestTiDBBatchPendingTiFlashCount`: unsigned
    /// values accept non-negative integers and reject decimal input.
    #[test]
    fn batch_pending_tiflash_count_validation_matches_go() {
        let sv = get_sys_var("tidb_batch_pending_tiflash_count").unwrap();
        assert_eq!(sv.validate("-10").unwrap().value, "0");
        assert_eq!(sv.validate("9999").unwrap().value, "9999");
        assert_eq!(sv.validate("1.5"), Err(ValidationError::WrongType));
    }

    /// Transcreated from Go `TestTiFlashMaxBytes` and
    /// `TestTiFlashMemQuotaQueryPerNode`: signed byte quotas clamp negative
    /// values to -1 and retain ordinary values, but do not accept an integer
    /// outside the signed 64-bit domain.
    #[test]
    fn tiflash_signed_quota_validation_matches_go() {
        for name in [
            "tidb_max_bytes_before_tiflash_external_join",
            "tidb_max_bytes_before_tiflash_external_group_by",
            "tidb_max_bytes_before_tiflash_external_sort",
            "tiflash_mem_quota_query_per_node",
        ] {
            let sv = get_sys_var(name).unwrap();
            assert_eq!(sv.validate("-10").unwrap().value, "-1", "{name}");
            assert_eq!(sv.validate("100").unwrap().value, "100", "{name}");
            assert!(sv.validate("9223372036854775808").is_err(), "{name}");
        }
    }

    /// Transcreated from Go `TestTiFlashQuerySpillRatio`: the generic float
    /// range is narrowed by the variable-specific closure to [0, 0.85].
    #[test]
    fn tiflash_query_spill_ratio_validation_matches_go() {
        let sv = get_sys_var("tiflash_query_spill_ratio").unwrap();
        assert_eq!(sv.validate("-10").unwrap().value, "0");
        assert_eq!(
            sv.validate("100"),
            Err(ValidationError::Refused(
                "The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85".into()
            ))
        );
        assert_eq!(
            sv.validate("0.9"),
            Err(ValidationError::Refused(
                "The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85".into()
            ))
        );
        assert_eq!(sv.validate("0.85").unwrap().value, "0.85");
    }

    /// Transcreated from Go `TestTiFlashHashJoinVersion`: only the legacy and
    /// optimized spellings are accepted, case-insensitively.
    #[test]
    fn tiflash_hash_join_version_validation_matches_go() {
        let sv = get_sys_var("tiflash_hash_join_version").unwrap();
        for value in ["legacy", "optimized", "Legacy", "Optimized", "LegaCy"] {
            assert!(sv.validate(value).is_ok(), "{value}");
        }
        assert_eq!(
            sv.validate("invalid"),
            Err(ValidationError::Refused(
                "incorrect value: `invalid`. tiflash_hash_join_version options: legacy, optimized"
                    .into()
            ))
        );
    }

    /// Transcreated from Go `TestTiDBMemQuotaQuery`: signed query memory is
    /// unlimited at -1, with lower values clamped to that sentinel.
    #[test]
    fn mem_quota_query_validation_matches_go() {
        let sv = get_sys_var("tidb_mem_quota_query").unwrap();
        for scope in [SCOPE_GLOBAL, SCOPE_SESSION] {
            assert_eq!(
                sv.validate_in_scope("33554432", scope).unwrap().value,
                "33554432"
            );
            assert_eq!(sv.validate_in_scope("-2", scope).unwrap().value, "-1");
        }
    }

    /// Transcreated from Go `TestTiDBQueryLogMaxLen`: the global log-length
    /// limit clamps into [0, 1 GiB].
    #[test]
    fn query_log_max_len_validation_matches_go() {
        let sv = get_sys_var("tidb_query_log_max_len").unwrap();
        assert_eq!(
            sv.validate_in_scope("33554432", SCOPE_GLOBAL)
                .unwrap()
                .value,
            "33554432"
        );
        assert_eq!(
            sv.validate_in_scope("1073741825", SCOPE_GLOBAL)
                .unwrap()
                .value,
            "1073741824"
        );
        assert_eq!(sv.validate_in_scope("-2", SCOPE_GLOBAL).unwrap().value, "0");
    }

    /// Transcreated from Go `TestTiDBCommitterConcurrency`: the global
    /// committer worker count clamps into [1, 10000].
    #[test]
    fn committer_concurrency_validation_matches_go() {
        let sv = get_sys_var("tidb_committer_concurrency").unwrap();
        assert_eq!(
            sv.validate_in_scope("1024", SCOPE_GLOBAL).unwrap().value,
            "1024"
        );
        assert_eq!(
            sv.validate_in_scope("10001", SCOPE_GLOBAL).unwrap().value,
            "10000"
        );
        assert_eq!(sv.validate_in_scope("0", SCOPE_GLOBAL).unwrap().value, "1");
    }

    /// Transcreated from Go `TestTiDBDDLFlashbackConcurrency`: the global
    /// DDL flashback worker count clamps into [1, 256].
    #[test]
    fn ddl_flashback_concurrency_validation_matches_go() {
        let sv = get_sys_var("tidb_ddl_flashback_concurrency").unwrap();
        assert_eq!(
            sv.validate_in_scope("128", SCOPE_GLOBAL).unwrap().value,
            "128"
        );
        assert_eq!(
            sv.validate_in_scope("257", SCOPE_GLOBAL).unwrap().value,
            "256"
        );
        assert_eq!(sv.validate_in_scope("0", SCOPE_GLOBAL).unwrap().value, "1");
    }

    /// Transcreated from Go `TestDDLWorkers`: both DDL reorg controls clamp
    /// out-of-range unsigned values to their registered bounds and preserve
    /// values inside the range.
    #[test]
    fn ddl_reorg_worker_and_batch_bounds_match_go() {
        let workers = get_sys_var("tidb_ddl_reorg_worker_cnt").unwrap();
        assert_eq!(
            workers
                .validate_in_scope("-100", SCOPE_GLOBAL)
                .unwrap()
                .value,
            "1"
        );
        assert_eq!(
            workers
                .validate_in_scope("1234", SCOPE_GLOBAL)
                .unwrap()
                .value,
            "256"
        );
        assert_eq!(
            workers
                .validate_in_scope("100", SCOPE_GLOBAL)
                .unwrap()
                .value,
            "100"
        );

        let batch = get_sys_var("tidb_ddl_reorg_batch_size").unwrap();
        assert_eq!(
            batch.validate_in_scope("10", SCOPE_GLOBAL).unwrap().value,
            "32"
        );
        assert_eq!(
            batch
                .validate_in_scope("999999", SCOPE_GLOBAL)
                .unwrap()
                .value,
            "10240"
        );
        assert_eq!(
            batch.validate_in_scope("100", SCOPE_GLOBAL).unwrap().value,
            "100"
        );
    }

    /// Transcreated from Go `TestSetJobScheduleWindow`: TTL schedule globals
    /// normalize short UTC clock values into the full `HH:MM +0000` form.
    #[test]
    fn ttl_job_schedule_window_validation_matches_go() {
        for name in [
            "tidb_ttl_job_schedule_window_start_time",
            "tidb_ttl_job_schedule_window_end_time",
        ] {
            let sv = get_sys_var(name).unwrap();
            assert_eq!(
                sv.validate_in_scope("16:11", SCOPE_GLOBAL).unwrap().value,
                "16:11 +0000",
                "{name}"
            );
            assert_eq!(
                sv.validate_in_scope("16:11 +0000", SCOPE_GLOBAL)
                    .unwrap()
                    .value,
                "16:11 +0000",
                "{name}"
            );
            assert!(
                sv.validate_in_scope("25:00", SCOPE_GLOBAL).is_err(),
                "{name}"
            );
        }
    }

    /// Transcreated from Go `TestTiDBServerMemoryLimitSessMinSize`: byte
    /// suffixes are converted to decimal bytes and positive values below 128
    /// are clamped with the truncation flag.
    #[test]
    fn server_memory_limit_session_min_size_validation_matches_go() {
        let sv = get_sys_var("tidb_server_memory_limit_sess_min_size").unwrap();
        let small = sv.validate("100").unwrap();
        assert_eq!(small.value, "128");
        assert!(small.truncated);
        assert_eq!(sv.validate("123456").unwrap().value, "123456");
        assert_eq!(sv.validate("123MB").unwrap().value, "128974848");
        assert_eq!(
            sv.validate("18446744073709551615").unwrap().value,
            "18446744073709551615"
        );
        assert_eq!(sv.validate("700MBaa"), Err(ValidationError::WrongType));
        for invalid in ["32b", "32kb", "32.5KiB", "1e2KiB"] {
            assert_eq!(
                sv.validate(invalid),
                Err(ValidationError::WrongType),
                "{invalid}"
            );
        }
    }

    /// Transcreated from Go `TestTiDBServerMemoryLimit`: the memory-limit
    /// parser preserves valid unit/decimal spellings and clamps a positive
    /// sub-512MiB value to `512MB`.
    #[test]
    fn server_memory_limit_validation_matches_go() {
        let sv = get_sys_var("tidb_server_memory_limit").unwrap();
        let small = sv.validate("100MB").unwrap();
        assert_eq!(small.value, "512MB");
        assert!(small.truncated);
        assert_eq!(sv.validate("0").unwrap().value, "0");
        assert_eq!(
            sv.validate("18446744073709551615").unwrap().value,
            "18446744073709551615"
        );
        assert_eq!(sv.validate("1073741824").unwrap().value, "1073741824");
        match sv.validate("123aaa123") {
            Err(ValidationError::SqlError(error)) => {
                assert_eq!(
                    error.code,
                    tidb_error::mysql::errcode::ErrTruncatedWrongValue
                );
                assert_eq!(
                    error.message,
                    "Truncated incorrect tidb_server_memory_limit value: '123aaa123'"
                );
            }
            other => panic!("expected Go's 1292 error, got {other:?}"),
        }
    }

    /// Transcreated from Go `TestTiDBServerMemoryLimitGCTrigger`: decimal
    /// fractions and integer percentages normalize to a fraction, with the
    /// lower bound and the percent parser's exclusive 100% boundary enforced.
    #[test]
    fn server_memory_limit_gc_trigger_validation_matches_go() {
        let sv = get_sys_var("tidb_server_memory_limit_gc_trigger").unwrap();
        assert_eq!(sv.validate("0.8").unwrap().value, "0.8");
        assert_eq!(sv.validate("90%").unwrap().value, "0.9");
        assert_eq!(sv.validate("99%").unwrap().value, "0.99");
        assert!(matches!(
            sv.validate("0.51"),
            Err(ValidationError::Refused(message))
                if message.contains("gogc_tuner_threshold + 0.05")
        ));
        let threshold_low = |name: &str| {
            (name == "tidb_gogc_tuner_threshold").then(|| "0.4".to_owned())
        };
        assert_eq!(
            sv.validate_in_scope_with_lookup("51%", SCOPE_GLOBAL, Some(&threshold_low))
                .unwrap()
                .value,
            "0.51"
        );
        assert_eq!(sv.validate("100%"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("101%"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("0.5"), Err(ValidationError::WrongValue));
    }

    /// Transcreated from Go `TestDefaultMemoryDebugModeValue`: both memory
    /// debug controls retain the zero default.
    #[test]
    fn memory_debug_mode_defaults_match_go() {
        for name in [
            "tidb_memory_debug_mode_min_heap_inuse",
            "tidb_memory_debug_mode_alarm_ratio",
        ] {
            assert_eq!(get_sys_var(name).unwrap().value, "0", "{name}");
        }
    }

    /// Transcreated from Go `TestTimestamp`: values below the minimum clamp
    /// with the truncation flag, values above the Int32 maximum are rejected,
    /// and the upper bound itself remains valid.
    #[test]
    fn timestamp_validation_matches_go() {
        let sv = get_sys_var("timestamp").unwrap();
        let truncated = sv.validate("-5").unwrap();
        assert_eq!(truncated.value, "0");
        assert!(truncated.truncated);
        assert_eq!(sv.validate("3147483698"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("2147483648"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("2147483647").unwrap().value, "2147483647");
    }

    /// Transcreated from Go `TestTiFlashMaxBytes`: these carry
    /// `AllowAutoValue`, so a negative clamps to -1, and a value past
    /// `i64::MAX` cannot convert and is an error.
    #[test]
    fn tiflash_max_bytes() {
        for name in [
            "tidb_max_bytes_before_tiflash_external_join",
            "tidb_max_bytes_before_tiflash_external_group_by",
            "tidb_max_bytes_before_tiflash_external_sort",
        ] {
            let sv = get_sys_var(name).unwrap();
            assert_eq!(sv.validate("-10").unwrap().value, "-1", "{name}");
            assert_eq!(sv.validate("100").unwrap().value, "100", "{name}");
            // i64::MAX + 1 is out of the Int64 range, so it cannot convert.
            assert_eq!(
                sv.validate("9223372036854775808"),
                Err(ValidationError::WrongType),
                "{name}"
            );
        }
    }

    /// Transcreated from Go `TestTiDBMultiStatementMode`: an enum accepts a
    /// name in any case or its ordinal position, and answers with the
    /// canonical name.
    #[test]
    fn tidb_multi_statement_mode() {
        let sv = get_sys_var("tidb_multi_statement_mode").unwrap();
        assert_eq!(sv.validate("on").unwrap().value, "ON");
        assert_eq!(sv.validate("0").unwrap().value, "OFF");
        assert_eq!(sv.validate("Warn").unwrap().value, "WARN");
    }

    /// Go `TestSecureAuth`: the compatibility switch permanently rejects
    /// OFF with ErrWrongValueForVar while accepting ON.
    #[test]
    fn secure_auth_rejects_off_and_accepts_on() {
        let sv = get_sys_var("secure_auth").unwrap();
        assert_eq!(sv.validate("OFF"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("ON").unwrap().value, "ON");
    }

    #[test]
    fn allow_empty_tables_name_live_registry_entries() {
        for name in ALLOW_EMPTY_VARS.iter().chain(ALLOW_EMPTY_ALL_VARS) {
            assert!(
                get_sys_var(name).is_some(),
                "the hand-maintained allow-empty table names no registry entry: {name}"
            );
        }
        assert_eq!(
            get_sys_var("tidb_capture_plan_baselines")
                .unwrap()
                .validate_in_scope("", SCOPE_GLOBAL)
                .unwrap()
                .value,
            ""
        );
    }

    /// Go's `validateReadConsistencyLevel` (`session.go:702`): only
    /// `strict`/`weak` in any case; stored as typed; anything else is
    /// `ErrWrongTypeForVar` (1232).
    #[test]
    fn read_consistency_whitelist_matches_go() {
        let sv = get_sys_var("tidb_read_consistency").unwrap();
        assert_eq!(sv.validate("strict").unwrap().value, "strict");
        assert_eq!(sv.validate("WEAK").unwrap().value, "WEAK");
        assert_eq!(sv.validate("bogus"), Err(ValidationError::WrongType));
        assert_eq!(sv.validate(""), Err(ValidationError::WrongType));
    }

    /// Go `variable/tests/variable_test.go`'s primitive validation cases:
    /// numeric types clamp with a truncation marker, bools accept only the
    /// documented spellings, and enums accept names or ordinal positions.
    #[test]
    fn primitive_type_validation_matches_go() {
        let int = SysVarDef {
            name: "mynewsysvar",
            var_type: VarType::Int,
            min_value: 10,
            max_value: 300,
            allow_auto_value: true,
            ..SysVarDef::PLACEHOLDER
        };
        assert_eq!(int.validate("301").unwrap().value, "300");
        assert!(int.validate("301").unwrap().truncated);
        assert_eq!(int.validate("5").unwrap().value, "10");
        assert_eq!(int.validate("-1").unwrap().value, "-1");
        assert_eq!(int.validate("oN"), Err(ValidationError::WrongType));

        let unsigned = SysVarDef {
            var_type: VarType::Unsigned,
            min_value: 10,
            max_value: 300,
            allow_auto_value: true,
            ..int
        };
        assert_eq!(unsigned.validate("301").unwrap().value, "300");
        assert_eq!(unsigned.validate("-301").unwrap().value, "10");
        assert_eq!(unsigned.validate("-ERR"), Err(ValidationError::WrongType));
        assert_eq!(unsigned.validate("-1").unwrap().value, "-1");

        let float = SysVarDef {
            var_type: VarType::Float,
            min_value: 2,
            max_value: 7,
            ..SysVarDef::PLACEHOLDER
        };
        assert_eq!(float.validate("1.1").unwrap().value, "2");
        assert_eq!(float.validate("22").unwrap().value, "7");
        assert_eq!(float.validate("stringval"), Err(ValidationError::WrongType));

        let boolean = SysVarDef {
            var_type: VarType::Bool,
            auto_convert_negative_bool: true,
            ..SysVarDef::PLACEHOLDER
        };
        assert_eq!(boolean.validate("0").unwrap().value, "OFF");
        assert_eq!(boolean.validate("1").unwrap().value, "ON");
        assert_eq!(boolean.validate("-1").unwrap().value, "ON");
        assert_eq!(boolean.validate("0.000"), Err(ValidationError::WrongValue));

        let enumeration = SysVarDef {
            var_type: VarType::Enum,
            possible_values: &["OFF", "ON", "AUTO"],
            ..SysVarDef::PLACEHOLDER
        };
        assert_eq!(enumeration.validate("oFf").unwrap().value, "OFF");
        assert_eq!(enumeration.validate("2").unwrap().value, "AUTO");
        assert_eq!(
            enumeration.validate("randomstring"),
            Err(ValidationError::WrongValue)
        );
    }

    /// Go `TestSysVar` reads `runtime.GOOS` and `runtime.GOARCH` for the
    /// compile-platform variables; the Rust registry must not retain the
    /// developer machine's captured values in cross-compiled builds.
    #[test]
    fn compile_platform_sysvars_follow_the_target() {
        assert_eq!(
            get_sys_var("version_compile_os").unwrap().value,
            std::env::consts::OS
        );
        assert_eq!(
            get_sys_var("version_compile_machine").unwrap().value,
            std::env::consts::ARCH
        );
    }

    /// Go `TestTimeValidation` and `TestDurationValidation`: time values are
    /// expanded to the full offset form, while durations clamp and render with
    /// Go's `time.Duration.String()` spelling.
    #[test]
    fn time_and_duration_validation_match_go() {
        let time = SysVarDef {
            var_type: VarType::Time,
            ..SysVarDef::PLACEHOLDER
        };
        assert_eq!(time.validate("23:59 +0000").unwrap().value, "23:59 +0000");
        assert_eq!(time.validate("3:00 +0000").unwrap().value, "03:00 +0000");
        assert_eq!(time.validate("0.000"), Err(ValidationError::WrongType));

        let duration = SysVarDef {
            var_type: VarType::Duration,
            min_value: 1_000_000_000,
            max_value: 3_600_000_000_000,
            ..SysVarDef::PLACEHOLDER
        };
        assert_eq!(duration.validate("1hr"), Err(ValidationError::WrongType));
        assert_eq!(duration.validate("1ms").unwrap().value, "1s");
        assert_eq!(duration.validate("2h10m").unwrap().value, "1h0m0s");

        let retention = get_sys_var("tidb_plan_replayer_file_retention_time").unwrap();
        assert_eq!(
            retention.validate("8761h").unwrap().value,
            "8760h0m0s",
            "Go caps plan-replayer retention at one year"
        );
    }

    /// Go `TestValidate`'s fallback-engine matrix: only TiFlash is accepted,
    /// with whitespace trimming and duplicate suppression.
    #[test]
    fn allow_fallback_to_tikv_validation_matches_go() {
        let sv = get_sys_var(tidb_vardef::tidb_vars::TIDB_ALLOW_FALLBACK_TO_TIKV).unwrap();
        assert_eq!(
            sv.validate(""),
            Ok(Validated {
                value: String::new(),
                truncated: false,
            })
        );
        assert_eq!(sv.validate("tiflash").unwrap().value, "tiflash");
        assert_eq!(sv.validate("  tiflash  ").unwrap().value, "tiflash");
        assert_eq!(sv.validate("tiflash,tiflash").unwrap().value, "tiflash");
        for value in ["tikv", "tidb", "tiflash,tikv,tidb"] {
            assert_eq!(
                sv.validate(value),
                Err(ValidationError::WrongValue),
                "{value}"
            );
        }
    }

    /// Go `TestValidateStmtSummary`'s global-scope matrix: empty values are
    /// rejected, while the integer controls clamp the same out-of-range
    /// inputs instead of refusing the assignment.
    #[test]
    fn stmt_summary_validation_matches_go() {
        let cases = [
            (tidb_vardef::tidb_vars::TIDB_ENABLE_STMT_SUMMARY, "", true),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_INTERNAL_QUERY,
                "",
                true,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_REFRESH_INTERVAL,
                "",
                true,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_REFRESH_INTERVAL,
                "0",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_REFRESH_INTERVAL,
                "99999999999",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_HISTORY_SIZE,
                "",
                true,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_HISTORY_SIZE,
                "0",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_HISTORY_SIZE,
                "-1",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_HISTORY_SIZE,
                "99999999",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_STMT_COUNT,
                "",
                true,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_STMT_COUNT,
                "0",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_STMT_COUNT,
                "99999999",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_SQL_LENGTH,
                "",
                true,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_SQL_LENGTH,
                "0",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_SQL_LENGTH,
                "-1",
                false,
            ),
            (
                tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_SQL_LENGTH,
                "99999999999",
                false,
            ),
        ];
        for (name, value, should_error) in cases {
            let result = get_sys_var(name)
                .unwrap()
                .validate_in_scope(value, SCOPE_GLOBAL);
            assert_eq!(result.is_err(), should_error, "{name}={value}");
        }
    }

    /// Go's mpp_exchange_compression_mode Validation (`sysvar.go:3308`):
    /// the four mode names pass (case-insensitively, stored as typed) and
    /// anything else is refused with the option list, reporting as 1105.
    #[test]
    fn mpp_exchange_compression_mode_whitelist_matches_go() {
        let sv = get_sys_var("mpp_exchange_compression_mode").unwrap();
        assert_eq!(sv.validate("UNSPECIFIED").unwrap().value, "UNSPECIFIED");
        assert_eq!(sv.validate("fast").unwrap().value, "fast");
        assert_eq!(
            sv.validate("HIGH_COMPRESSION").unwrap().value,
            "HIGH_COMPRESSION"
        );
        let refused = sv.validate("bogus");
        match refused {
            Err(ValidationError::Refused(message)) => {
                assert!(message.contains("incorrect value: `bogus`"), "{message}");
                assert!(
                    message.contains("NONE, FAST, HIGH_COMPRESSION, UNSPECIFIED"),
                    "{message}"
                );
            }
            other => panic!("expected a refused error, got {other:?}"),
        }
    }

    /// Go's init_connect Validation (`sysvar.go:704`): the value must parse
    /// as SQL; a parse failure is `ErrWrongTypeForVar` (1232) and the empty
    /// value passes (zero statements).
    #[test]
    fn init_connect_value_must_parse_as_sql_like_go() {
        let sv = get_sys_var("init_connect").unwrap();
        assert_eq!(
            sv.validate("SET autocommit = 1").unwrap().value,
            "SET autocommit = 1"
        );
        assert_eq!(
            sv.validate(""),
            Ok(Validated {
                value: String::new(),
                truncated: false
            })
        );
        assert_eq!(
            sv.validate("THIS IS NOT SQL ~~~"),
            Err(ValidationError::WrongType)
        );
    }

    /// Go's mpp_version Validation (`sysvar.go:3335`): UNSPECIFIED and the
    /// integer range [-1, 3] pass (stored as typed); anything else is
    /// refused with the option list.
    #[test]
    fn mpp_version_whitelist_matches_go() {
        let sv = get_sys_var("mpp_version").unwrap();
        assert_eq!(sv.validate("UNSPECIFIED").unwrap().value, "UNSPECIFIED");
        assert_eq!(sv.validate("0").unwrap().value, "0");
        assert_eq!(sv.validate("3").unwrap().value, "3");
        assert_eq!(sv.validate("-1").unwrap().value, "-1");
        match sv.validate("4") {
            Err(ValidationError::Refused(message)) => {
                assert!(
                    message.contains(
                        "incorrect value: `4`. mpp_version options: -1 (unspecified), 0, 1, 2, 3"
                    ),
                    "{message}"
                );
            }
            other => panic!("expected a refused error, got {other:?}"),
        }
        match sv.validate("bogus") {
            Err(ValidationError::Refused(message)) => {
                assert!(message.contains("bogus"), "{message}");
            }
            other => panic!("expected a refused error, got {other:?}"),
        }
    }

    /// The mem-arbitrator whitelist shapes (`sysvar.go`, the
    /// tidb_mem_arbitrator_* entries).
    #[test]
    fn mem_arbitrator_validations_match_go() {
        let mode = get_sys_var("tidb_mem_arbitrator_mode").unwrap();
        assert_eq!(mode.validate("STANDARD").unwrap().value, "standard");
        assert_eq!(mode.validate("disable").unwrap().value, "disable");
        assert!(matches!(
            mode.validate("bogus"),
            Err(ValidationError::Refused(_))
        ));

        let wait_averse = get_sys_var("tidb_mem_arbitrator_wait_averse").unwrap();
        for ok in ["0", "1", "nolimit"] {
            assert_eq!(wait_averse.validate(ok).unwrap().value, ok);
        }
        assert!(matches!(
            wait_averse.validate("2"),
            Err(ValidationError::Refused(_))
        ));

        let reserved = get_sys_var("tidb_mem_arbitrator_query_reserved").unwrap();
        assert_eq!(reserved.validate("0").unwrap().value, "0");
        assert_eq!(reserved.validate("2").unwrap().value, "2");
        assert!(matches!(
            reserved.validate("1"),
            Err(ValidationError::Refused(_))
        ));
        for invalid in ["9223372036854775808", "18446744073709551615"] {
            assert!(
                matches!(reserved.validate(invalid), Err(ValidationError::Refused(_))),
                "{invalid}"
            );
        }

        let soft_limit = get_sys_var("tidb_mem_arbitrator_soft_limit").unwrap();
        assert_eq!(soft_limit.validate("0").unwrap().value, "0");
        assert_eq!(soft_limit.validate("AUTO").unwrap().value, "auto");
        assert_eq!(soft_limit.validate("0.8").unwrap().value, "0.8");
        assert_eq!(soft_limit.validate("1").unwrap().value, "1");
        assert_eq!(soft_limit.validate("2").unwrap().value, "2");
        for invalid in ["-1", "0.0", "1.1", "bogus"] {
            assert!(matches!(
                soft_limit.validate(invalid),
                Err(ValidationError::Refused(_))
            ));
        }
    }

    /// Go's tidb_gogc_tuner_threshold Validation (`sysvar.go:1270`):
    /// non-numeric input silently becomes the 0.6 default, numbers store as
    /// their shortest float text, and the range guards are dead code.
    #[test]
    fn gogc_tuner_threshold_falls_back_to_the_default_like_go() {
        let sv = get_sys_var("tidb_gogc_tuner_threshold").unwrap();
        assert_eq!(sv.validate("0.3").unwrap().value, "0.3");
        assert_eq!(sv.validate("bogus").unwrap().value, "0.6");
        assert_eq!(sv.validate("-5").unwrap().value, "-5");
    }

    /// The registry's concrete bool entry follows the same Go conversion
    /// rules as the synthetic `TestBoolValidation` cases above.
    /// Go's allow-fallback engine whitelist (`sysvar.go:2657`) and the
    /// analyze skip column types whitelist (`varsutil.go:501`).
    #[test]
    fn fallback_and_skip_column_type_whitelists_match_go() {
        let fallback = get_sys_var("tidb_allow_fallback_to_tikv").unwrap();
        assert_eq!(fallback.validate("").unwrap().value, "");
        // Dedup is by store type: the second (any-case) occurrence is a
        // no-op, so the normalized form lists tiflash once.
        assert_eq!(
            fallback.validate(" tiflash , TIFLASH ").unwrap().value,
            "tiflash"
        );
        assert!(matches!(
            fallback.validate(" TIKV "),
            Err(ValidationError::WrongValue)
        ));
        assert!(matches!(
            fallback.validate("tiflash,tikv"),
            Err(ValidationError::WrongValue)
        ));

        let skip = get_sys_var("tidb_analyze_skip_column_types").unwrap();
        assert_eq!(skip.validate("").unwrap().value, "");
        assert_eq!(skip.validate(" JSON , Blob ").unwrap().value, "json,blob");
        assert!(matches!(
            skip.validate("varchar"),
            Err(ValidationError::WrongValue)
        ));
    }

    /// Go's max_dist_task_nodes zero refusal and the evolve-plan-baselines
    /// ON refusal (bare errors, 1105).
    #[test]
    fn max_dist_task_nodes_and_evolve_baselines_match_go() {
        let nodes = get_sys_var("tidb_max_dist_task_nodes").unwrap();
        assert_eq!(nodes.validate("-1").unwrap().value, "-1");
        assert_eq!(nodes.validate("128").unwrap().value, "128");
        match nodes.validate("0") {
            Err(ValidationError::Refused(message)) => {
                assert!(message.contains("-1 or [1, 128]"), "{message}");
            }
            other => panic!("expected a refused error, got {other:?}"),
        }

        let evolve = get_sys_var("tidb_evolve_plan_baselines").unwrap();
        assert_eq!(evolve.validate("OFF").unwrap().value, "OFF");
        match evolve.validate("ON") {
            Err(ValidationError::Refused(message)) => {
                assert!(
                    message.contains("Cannot enable baseline evolution"),
                    "{message}"
                );
            }
            other => panic!("expected a refused error, got {other:?}"),
        }
    }

    fn bool_validation() {
        let sv = get_sys_var("autocommit").unwrap();
        assert_eq!(sv.var_type, VarType::Bool);
        for (input, want) in [("on", "ON"), ("OFF", "OFF"), ("1", "ON"), ("0", "OFF")] {
            assert_eq!(sv.validate(input).unwrap().value, want, "{input}");
        }
        assert_eq!(sv.validate("2"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("yes"), Err(ValidationError::WrongValue));
    }

    /// Go `TestIsNoop`: MySQL compatibility variables are marked no-op while
    /// ordinary optimizer/session variables remain active.
    #[test]
    fn noop_metadata_matches_go() {
        assert!(get_sys_var("tx_read_only").unwrap().is_noop());
        assert!(get_sys_var("read_only").unwrap().is_noop());
        assert!(get_sys_var("innodb_fast_shutdown").unwrap().is_noop());
        assert!(!get_sys_var("tidb_multi_statement_mode").unwrap().is_noop());
        assert!(!get_sys_var("default_password_lifetime").unwrap().is_noop());
    }

    /// Go `TestValidateWithRelaxedValidation`: normalization survives while
    /// type/closure errors are swallowed and the original text is returned.
    #[test]
    fn relaxed_validation_matches_go() {
        assert_eq!(
            get_sys_var("secure_auth")
                .unwrap()
                .validate_with_relaxed_validation("1", SCOPE_GLOBAL),
            "ON"
        );
        assert_eq!(
            get_sys_var("tidb_analyze_version")
                .unwrap()
                .validate_with_relaxed_validation("1", SCOPE_SESSION),
            "1"
        );
        assert_eq!(
            get_sys_var("default_authentication_plugin")
                .unwrap()
                .validate_with_relaxed_validation("RandomText", SCOPE_GLOBAL),
            "RandomText"
        );
        assert_eq!(
            get_sys_var("init_connect")
                .unwrap()
                .validate_with_relaxed_validation("RandomText - should be valid SQL", SCOPE_GLOBAL),
            "RandomText - should be valid SQL"
        );
    }

    /// Pinned Go `TestEnableAutoAnalyzePriorityQueue`: ON remains accepted,
    /// while OFF reaches the deprecated-variable validation refusal.
    #[test]
    fn auto_analyze_priority_queue_is_always_enabled() {
        let sv = get_sys_var("tidb_enable_auto_analyze_priority_queue").unwrap();
        assert_eq!(sv.validate("ON").unwrap().value, "ON");
        assert_eq!(
            sv.validate("OFF"),
            Err(ValidationError::Refused(
                "tidb_enable_auto_analyze_priority_queue has been deprecated and TiDB will always use priority queue to schedule auto analyze"
                    .to_owned(),
            ))
        );
    }

    /// Pinned Go `TestTimeZone` plus the boundary cases in
    /// `varsutil_test.go`: names are case-sensitive, offsets are bounded, and
    /// only `SYSTEM` is case-folded before storage.
    #[test]
    fn time_zone_uses_go_parser_and_error() {
        let sv = get_sys_var("time_zone").unwrap();
        for value in [
            "America/Edmonton",
            "Europe/Helsinki",
            "America/New_York",
            "+10:00",
            "-6:00",
            "+14:00",
            "-12:59",
            "UTC",
            "+00:00",
        ] {
            assert_eq!(sv.validate(value).unwrap().value, value);
        }
        assert_eq!(sv.validate("system").unwrap().value, "SYSTEM");
        for invalid in ["America/EDMONTON", "+14:01", "-13:00", "6:00"] {
            let Err(ValidationError::SqlError(error)) = sv.validate(invalid) else {
                panic!("{invalid} must be ErrUnknownTimeZone");
            };
            assert_eq!(error.code, 1298, "{invalid}");
            assert_eq!(
                error.message,
                format!("Unknown or incorrect time zone: '{invalid}'")
            );
        }
    }

    /// A ScopeNone entry is read-only, which Go reports rather than storing.
    #[test]
    fn read_only_entries() {
        let version = get_sys_var("version").unwrap();
        assert_eq!(version.scope, SCOPE_NONE);
        assert!(version.is_read_only());
        let autocommit = get_sys_var("autocommit").unwrap();
        assert!(!autocommit.is_read_only());
        assert!(autocommit.has_session_scope() && autocommit.has_global_scope());
    }

    /// The defaults a connecting client reads come from the captured table.
    #[test]
    fn captured_defaults_match_go() {
        assert_eq!(get_sys_var("autocommit").unwrap().value, "ON");
        assert_eq!(
            get_sys_var("character_set_client").unwrap().value,
            "utf8mb4"
        );
        assert_eq!(
            get_sys_var("transaction_isolation").unwrap().value,
            "REPEATABLE-READ"
        );
        assert_eq!(get_sys_var("max_allowed_packet").unwrap().value, "67108864");
        assert!(get_sys_var("version").unwrap().value.starts_with("8.0.11"));
    }
}

/// Go routes `character_set_database` and `collation_database` through
/// `checkCharacterSet` (`varsutil.go:76`) and `checkCollation`
/// (`varsutil.go:57`): canonical-name resolution with 1115/1273 refusals,
/// and the empty charset value is `ErrWrongValueForVar` (1231).
#[test]
fn database_charset_and_collation_set_validation_matches_go() {
    let cs = get_sys_var("character_set_database").unwrap();
    assert_eq!(cs.validate("UTF8MB4").unwrap().value, "utf8mb4");
    assert!(matches!(
        cs.validate("bogus_charset"),
        Err(ValidationError::SqlError(_))
    ));
    assert_eq!(cs.validate(""), Err(ValidationError::WrongValue));

    let coll = get_sys_var("collation_database").unwrap();
    assert_eq!(
        coll.validate("UTF8MB4_GENERAL_CI").unwrap().value,
        "utf8mb4_general_ci"
    );
    assert!(matches!(
        coll.validate("bogus_collation"),
        Err(ValidationError::SqlError(_))
    ));
}

/// Go's runtime-filter Validations (`sysvar.go:3726-3751`): the type is a
/// comma-separated IN/MIN_MAX list (case-insensitive tokens, stored as
/// typed) and the mode is exactly OFF or LOCAL; both refusals are the
/// option-list messages reporting as 1105.
#[test]
fn runtime_filter_validations_match_go() {
    let rf_type = get_sys_var("tidb_runtime_filter_type").unwrap();
    assert_eq!(rf_type.validate("IN").unwrap().value, "IN");
    assert_eq!(rf_type.validate("in,min_max").unwrap().value, "in,min_max");
    assert_eq!(
        rf_type.validate("IN, MIN_MAX").unwrap().value,
        "IN, MIN_MAX"
    );
    match rf_type.validate("BOGUS") {
        Err(ValidationError::Refused(message)) => {
            assert!(message.contains("incorrect value: BOGUS"), "{message}");
            assert!(message.contains("only support IN and MIN_MAX"), "{message}");
        }
        other => panic!("expected a refused error, got {other:?}"),
    }

    let rf_mode = get_sys_var("tidb_runtime_filter_mode").unwrap();
    assert_eq!(rf_mode.validate("OFF").unwrap().value, "OFF");
    assert_eq!(rf_mode.validate("LOCAL").unwrap().value, "LOCAL");
    assert!(matches!(
        rf_mode.validate("local"),
        Err(ValidationError::Refused(_))
    ));
}

/// Go's index-join-v2 always-on rule (`sysvar.go:2874`) and the schema
/// cache size parse-and-clamp (`varsutil.go:537`): falsy sets are refused
/// with the always-enabled message, sizes below 64MB clamp up, sizes above
/// i64::MAX clamp down, and unparseable values are 1292.
#[test]
fn index_join_v2_and_schema_cache_size_match_go() {
    let v2 = get_sys_var("tidb_opt_index_join_build_v2").unwrap();
    assert_eq!(v2.validate("ON").unwrap().value, "ON");
    assert_eq!(v2.validate("1").unwrap().value, "ON");
    match v2.validate("OFF") {
        Err(ValidationError::Refused(message)) => {
            assert!(
                message.contains("always enabled and cannot be turned off"),
                "{message}"
            );
        }
        other => panic!("expected a refused error, got {other:?}"),
    }

    let scs = get_sys_var("tidb_schema_cache_size").unwrap();
    assert_eq!(scs.validate("64MB").unwrap().value, "64MB");
    assert_eq!(scs.validate("1MB").unwrap().value, "64MB");
    assert_eq!(scs.validate("134217728").unwrap().value, "134217728");
    assert!(matches!(
        scs.validate("not-a-size"),
        Err(ValidationError::SqlError(_))
    ));
}

/// Go's `checkIsolationLevel` (`varsutil.go:116`): SERIALIZABLE and
/// READ-UNCOMMITTED are refused with `ErrUnsupportedIsolationLevel` (8048)
/// unless the session's `tidb_skip_isolation_level_check` is ON, in which
/// case the set proceeds.
#[test]
fn tx_isolation_one_shot_matches_go() {
    let sv = get_sys_var("tx_isolation_one_shot").unwrap();
    let lookup_on =
        |name: &str| (name == "tidb_skip_isolation_level_check").then(|| "ON".to_owned());
    let lookup_off =
        |name: &str| (name == "tidb_skip_isolation_level_check").then(|| "OFF".to_owned());
    assert_eq!(
        sv.validate_in_scope_with_lookup("READ-COMMITTED", 1, Some(&lookup_on))
            .unwrap()
            .value,
        "READ-COMMITTED"
    );
    match sv.validate_in_scope_with_lookup("SERIALIZABLE", 1, Some(&lookup_off)) {
        Err(ValidationError::SqlError(error)) => assert_eq!(error.code, 8048),
        other => panic!("expected the 8048 refusal, got {other:?}"),
    }
    // Without a lookup the sibling counts as unset: refusal.
    assert!(matches!(
        sv.validate_in_scope_with_lookup("SERIALIZABLE", 1, None),
        Err(ValidationError::SqlError(_))
    ));
}
