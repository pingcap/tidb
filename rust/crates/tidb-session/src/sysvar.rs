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
//! GENERATED, NOT HAND-WRITTEN: every one of the 948 entries below was
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
    /// than adding a generated field to all 948 entries for one true value.
    #[must_use]
    pub fn is_internal_session_variable(&self) -> bool {
        self.name == "tidb_redact_log"
    }
}

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
/// binary search answered -- just without the per-probe comparisons. The keys
/// are engine constants, not attacker input, so the fixed-seed hasher from
/// [`tidb_util::fast_hash`] answers probes without SipHash's mixing rounds.
fn sys_var_index() -> &'static tidb_util::fast_hash::FxHashMap<&'static str, usize> {
    static INDEX: std::sync::OnceLock<tidb_util::fast_hash::FxHashMap<&'static str, usize>> =
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
    tidb_util::sem::effective_sysvar_default(definition.name)
        .unwrap_or_else(|| definition.value.to_owned())
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
    /// Go `ErrWrongValueForVar` (1231) where the rejected text is not the
    /// whole assigned value: `SET sql_mode = 'ANSI,BOGUS'` names `BOGUS`.
    WrongValueOf(String),
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

    /// Go `SysVar.ValidateFromType` including its `scope` argument, which only
    /// the empty-value escape hatch reads.
    pub fn validate_in_scope(&self, value: &str, scope: u8) -> Result<Validated, ValidationError> {
        let validated = self.normalize_by_type(value, scope)?;
        self.run_validation(validated, value)
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
            // Go's TypeTime and TypeDuration checks parse a clock time and a
            // Go duration; they are not ported, so those variables take their
            // value unchanged rather than being wrongly rejected.
            VarType::Time | VarType::Duration | VarType::Str => Ok(Validated {
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
        if self.name == "time_zone" && validated.value.eq_ignore_ascii_case("SYSTEM") {
            return Ok(Validated {
                value: "SYSTEM".to_owned(),
                truncated: validated.truncated,
            });
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
            // Go's `newInvalidModeErr` names the offending *token*, not the
            // whole assigned string.
            Err(invalid) => Err(ValidationError::WrongValueOf(invalid.value)),
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
        assert_eq!(SYS_VARS.len(), 948);
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

    /// Go `TestSecureAuth` does NOT pass against this layer, and this test
    /// pins why rather than hiding it.
    ///
    /// `secure_auth` rejects `OFF` from its per-variable `Validation`
    /// closure, which runs after `ValidateFromType`. Only `ValidateFromType`
    /// is ported (see the module doc), so the type check accepts `OFF` here
    /// where real TiDB answers
    /// "Variable 'secure_auth' can't be set to the value of 'OFF'".
    /// Porting the closures is the next unit for this file.
    #[test]
    fn per_variable_validation_closures_are_not_modelled() {
        let sv = get_sys_var("secure_auth").unwrap();
        // The declarative type check passes it, unlike real TiDB.
        assert_eq!(sv.validate("OFF").unwrap().value, "OFF");
        // The part that IS ported agrees with Go.
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

    /// Go `checkBoolSystemVar`: ON/OFF in any case, 0 and 1, and nothing else
    /// unless the variable converts negatives.
    #[test]
    fn bool_validation() {
        let sv = get_sys_var("autocommit").unwrap();
        assert_eq!(sv.var_type, VarType::Bool);
        for (input, want) in [("on", "ON"), ("OFF", "OFF"), ("1", "ON"), ("0", "OFF")] {
            assert_eq!(sv.validate(input).unwrap().value, want, "{input}");
        }
        assert_eq!(sv.validate("2"), Err(ValidationError::WrongValue));
        assert_eq!(sv.validate("yes"), Err(ValidationError::WrongValue));
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
