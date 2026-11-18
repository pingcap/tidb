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

//! Go `SysVar.Validation`: each variable's own check, kept with the
//! variable. A catalog entry names its function in
//! [`SysVarDef::validation`](super::SysVarDef::validation); the generic
//! [`SysVarDef::validate_in_scope_with_lookup`](super::SysVarDef::validate_in_scope_with_lookup)
//! runs it after `ValidateFromType`, as Go's `SysVar.Validate` does.
//!
//! Every function has the signature of Go's closure: the variable, the
//! type-normalized value, the original text, the scope, and a reader for
//! sibling variables (Go's `vars`).
#![allow(unused_variables, unused_mut)]

use super::*;

/// A sibling variable's current value, or `None` (Go reads `vars.systems`).
pub(super) type Lookup<'a> = Option<&'a dyn Fn(&str) -> Option<String>>;

fn conflict(message: &str) -> ValidationError {
    ValidationError::SqlError(SqlError::new_f(
        tidb_mysql::errors::errcode::ErrUnknown,
        "%s",
        &[],
        &[FormatArg::from(message)],
    ))
}

/// Go's `tidb_gogc_tuner_threshold` Validation (`sysvar.go`): after the
/// float type check, the threshold must stay 0.05 below
/// `gctuner.GlobalMemoryLimitTuner`'s percentage, which Go keeps equal to
/// `tidb_server_memory_limit_gc_trigger` (0.7 by default). The value is
/// stored as its shortest float text.
pub(super) fn tidb_gogc_tuner_threshold(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let float_value = validated.value.parse::<f64>().unwrap_or(0.6);
    let trigger = lookup
        .and_then(|read| read("tidb_server_memory_limit_gc_trigger"))
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.7);
    if trigger != 0.0 && trigger < float_value + 0.05 {
        return Err(ValidationError::Refused(
            "tidb_gogc_tuner_threshold should be less than tidb_server_memory_limit_gc_trigger - 0.05"
                .to_owned(),
        ));
    }
    Ok(Validated {
        value: format!("{float_value}"),
        truncated: validated.truncated,
    })
}

pub(super) fn tiflash_compute_dispatch_policy(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    tidb_tiflashcompute::DispatchPolicy::parse(&validated.value)
        .map_err(ValidationError::Refused)?;
    Ok(validated)
}

/// Go's `timestamp` validation (`sysvar.go`, the `vardef.Timestamp`
/// entry): `tidbOptFloat64(originalValue)` above `math.MaxInt32` is
/// `ErrWrongValueForVar`. The type check alone would have clamped it to
/// the bound and accepted it, which is the opposite outcome.
pub(super) fn timestamp(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if original
        .parse::<f64>()
        .is_ok_and(|value| value > f64::from(i32::MAX))
    {
        return Err(ValidationError::WrongValue);
    }
    Ok(validated)
}

/// Go's `time_zone` validation (`sysvar.go`, the `vardef.TimeZone`
/// entry): `SYSTEM` in any spelling canonicalizes to upper case, and
/// every other name is stored exactly as typed -- `SET time_zone='utc'`
/// reads back `utc`, not `UTC`.
pub(super) fn time_zone(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value.eq_ignore_ascii_case("SYSTEM") {
        return Ok(Validated {
            value: "SYSTEM".to_owned(),
            truncated: validated.truncated,
        });
    }
    tidb_timeutil::parse_time_zone(&validated.value)
        .map_err(|error| ValidationError::SqlError(error.to_sql_error()))?;
    Ok(validated)
}

/// Go's `secure_auth` validation keeps the deprecated compatibility
/// switch permanently enabled: OFF is ErrWrongValueForVar (1231),
/// while ON remains the normalized stored value.
pub(super) fn secure_auth(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value == "OFF" {
        return Err(ValidationError::WrongValue);
    }
    Ok(validated)
}

/// Go's `tidb_mpp_store_fail_ttl` compatibility variable is always
/// forced back to its `0s` default. Its Session hook emits a warning,
/// while the value returned by Validation is what the table stores.
pub(super) fn tidb_mpp_store_fail_ttl(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    Ok(Validated {
        value: "0s".to_owned(),
        truncated: validated.truncated,
    })
}

/// Go's column-tracking switch is retained as a deprecated global
/// compatibility variable but is now unconditionally ON. Normalize it
/// here so the shared table has the same observable value as Go's
/// custom GetGlobal hook after either boolean spelling.
pub(super) fn tidb_enable_column_tracking(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    Ok(Validated {
        value: "ON".to_owned(),
        truncated: validated.truncated,
    })
}

/// Go's `tidb_partition_prune_mode` closure upgrades the out-of-date
/// enum spellings (`static-only`/`dynamic-only`) before storing them.
/// The type metadata admits those spellings, but readers and the
/// session setter observe only the upgraded `static`/`dynamic` mode.
pub(super) fn tidb_partition_prune_mode(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let mode = crate::session_vars::PartitionPruneMode::from_str_value(&validated.value);
    let updated = mode.update();
    if !mode.valid() {
        return Err(ValidationError::WrongValue);
    }
    Ok(Validated {
        value: updated.as_str().to_owned(),
        truncated: validated.truncated,
    })
}

/// Go has retired partition-statistics concurrency: every accepted
/// assignment is stored and read back as `1`, while a non-1 request
/// emits a 1287 deprecation warning in the SQL session hook.
pub(super) fn tidb_merge_partition_stats_concurrency(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    Ok(Validated {
        value: "1".to_owned(),
        truncated: validated.truncated,
    })
}

/// Go's `tidb_opt_partial_ordered_index_for_topn` closure validates the
/// ORIGINAL spelling after enum type normalization: only DISABLE and
/// COST (case-insensitively) are accepted, and ordinal enum values such
/// as `0` are rejected with ErrWrongValueForVar. Store the canonical
/// upper-case mode returned by that closure.
pub(super) fn tidb_opt_partial_ordered_index_for_topn(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let mode = original.trim().to_ascii_uppercase();
    if matches!(mode.as_str(), "DISABLE" | "COST") {
        return Ok(Validated {
            value: mode,
            truncated: validated.truncated,
        });
    }
    Err(ValidationError::WrongValue)
}

/// Go's `tidb_foreign_key_check_in_shared_lock` Validation allows ON
/// on Classic, while NextGen requires the experimental configuration
/// gate `allow-enable-foreign-key-check-in-shared-lock`.
pub(super) fn tidb_foreign_key_check_in_shared_lock(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if tidb_config_kerneltype::is_next_gen()
        && validated.value == "ON"
        && !tidb_config::config_tree::config::get_global_config()
            .experimental
            .allow_enable_foreign_key_check_in_shared_lock
    {
        return Err(ValidationError::WrongValue);
    }
    Ok(validated)
}

/// Go's `tidb_enable_noop_functions` Validation refuses OFF while a
/// same-scope read-only/no-op variable remains ON. The lookup is
/// supplied by the session or GLOBAL writer; direct registry callers
/// fall back to each variable's declared default.
pub(super) fn tidb_enable_noop_functions(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value == "OFF" {
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
                    tidb_mysql::errors::errcode::ErrNotSupportedYet,
                    "%s = OFF is not supported when %s = ON",
                    &[],
                    &[FormatArg::from(def.name), FormatArg::from(incompatible)],
                )));
            }
        }
    }
    Ok(validated)
}

/// Go's nextgen-only `TestTiDBPessimisticTransactionFairLocking`
/// exercises the variable-specific Validation closure: ON is rejected
/// with ErrNotSupportedInNextGen (1235) and the normalized fallback is
/// OFF. Classic builds retain the ordinary boolean behavior.
pub(super) fn tidb_pessimistic_txn_fair_locking(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if tidb_config_kerneltype::is_next_gen() && validated.value == "ON" {
        return Err(ValidationError::SqlError(SqlError::new_f(
            tidb_mysql::errors::errcode::ErrNotSupportedYet,
            "%s is not supported in the next generation of TiDB",
            &[],
            &[FormatArg::from(def.name)],
        )));
    }
    Ok(validated)
}

/// Go's nextgen-only `TestTiDBDMLTypeInNextGen` keeps STANDARD
/// available but rejects BULK with the same 1235 compatibility error.
pub(super) fn tidb_dml_type(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if tidb_config_kerneltype::is_next_gen() && validated.value.eq_ignore_ascii_case("bulk") {
        return Err(ValidationError::SqlError(SqlError::new_f(
            tidb_mysql::errors::errcode::ErrNotSupportedYet,
            "%s is not supported in the next generation of TiDB",
            &[],
            &[FormatArg::from(def.name)],
        )));
    }
    Ok(validated)
}

/// Go's nextgen-only replica-read validation retains LEADER and maps
/// every other non-empty mode back to LEADER after returning 1235.
pub(super) fn tidb_replica_read(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if tidb_config_kerneltype::is_next_gen()
        && !validated.value.eq_ignore_ascii_case("leader")
        && !validated.value.is_empty()
    {
        return Err(ValidationError::SqlError(SqlError::new_f(
            tidb_mysql::errors::errcode::ErrNotSupportedYet,
            "%s is not supported in the next generation of TiDB",
            &[],
            &[FormatArg::from(def.name)],
        )));
    }
    Ok(validated)
}

/// Go's `tiflash_query_spill_ratio` keeps the generic float range
/// [0, 1], then narrows it in its variable-specific Validation closure
/// to [0, 0.85]. Values below zero have already been clamped to 0 by
/// ValidateFromType; values above 0.85 are refused with a bare 1105.
pub(super) fn tiflash_query_spill_ratio(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated
        .value
        .parse::<f64>()
        .is_ok_and(|value| value > 0.85)
    {
        return Err(ValidationError::Refused(
            "The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85".into(),
        ));
    }
    Ok(validated)
}

/// Go's `tidb_server_memory_limit` validation parses percentages and
/// binary-unit byte sizes, stores the parser's normalized spelling,
/// and clamps a positive limit below 512 MiB with a truncation warning.
/// The host total is a process fact, so a percentage is only resolved
/// when the runtime can provide it; byte forms remain deterministic.
pub(super) fn tidb_server_memory_limit(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let total = tidb_memory::mem_total().unwrap_or_default();
    let parsed =
        crate::varsutil::parse_memory_limit(total, &validated.value, original).map_err(|_| {
            ValidationError::SqlError(SqlError::new(
                tidb_mysql::errors::errcode::ErrTruncatedWrongValue,
                &[FormatArg::from(def.name), FormatArg::from(original)],
            ))
        })?;
    Ok(Validated {
        value: parsed.normalized,
        truncated: parsed.clamped || validated.truncated,
    })
}

/// Go's `tidb_server_memory_limit_sess_min_size` validation first
/// accepts an unsigned byte count, then falls back to its exact
/// integer binary-unit `parseByteSize` helper. Values in (0, 128) are
/// clamped with a truncation warning and the stored form is always the
/// decimal byte count, never the original suffix.
pub(super) fn tidb_server_memory_limit_sess_min_size(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
    Ok(Validated {
        value: bytes.to_string(),
        truncated: validated.truncated,
    })
}

/// Go's `tidb_server_memory_limit_gc_trigger` accepts either a
/// decimal fraction or an integer percentage below 100, stores a
/// canonical fraction, and admits only the [0.51, 1] range. It also
/// refuses values below the current GOGC tuner threshold + 0.05. The
/// sibling lookup supplies the pending GLOBAL value during a table
/// write; direct registry validation uses Go's 0.6 default.
pub(super) fn tidb_server_memory_limit_gc_trigger(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let text = validated.value.trim();
    let fraction = if let Some(percent) = text.strip_suffix('%') {
        let percent = percent.parse::<u64>().map_err(|_| {
            ValidationError::Refused(format!(
                "strconv.ParseFloat: parsing {text:?}: invalid syntax"
            ))
        })?;
        if percent == 0 || percent >= 100 {
            return Err(ValidationError::Refused(format!(
                "strconv.ParseFloat: parsing {text:?}: invalid syntax"
            )));
        }
        percent as f64 / 100.0
    } else {
        text.parse::<f64>().map_err(|_| {
            ValidationError::Refused(format!(
                "strconv.ParseFloat: parsing {text:?}: invalid syntax"
            ))
        })?
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
    Ok(Validated {
        value: fraction.to_string(),
        truncated: validated.truncated,
    })
}

/// Go's `tidb_disable_txn_auto_retry` is retained only as a
/// compatibility alias: assigning OFF emits warning 1287 but stores
/// ON, because automatic retry can no longer be disabled.
pub(super) fn tidb_disable_txn_auto_retry(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value == "OFF" {
        return Ok(Validated {
            value: "ON".to_owned(),
            truncated: validated.truncated,
        });
    }
    Ok(validated)
}

/// Go routes connection, server, and database collations through
/// `checkCollation` (`varsutil.go:57`): resolve the name through
/// `collate.GetCollationByName`, store the canonical spelling, and
/// refuse a miss or a collation the new framework does not implement
/// with 1273.
///
/// Shared by `collation_connection`, `collation_database`, `collation_server`.
pub(super) fn collations(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let collation =
        supported_collation(&validated.value, original).map_err(ValidationError::SqlError)?;
    Ok(Validated {
        value: collation.name,
        truncated: validated.truncated,
    })
}

/// Go's `default_collation_for_utf8mb4` validation first resolves the
/// name through the same registry, then admits only the three default
/// utf8mb4 collations retained for compatibility.  The latter refusal
/// is the registered TiDB error 3721 rather than a generic variable
/// error, so preserve its catalogued message and code here.
pub(super) fn default_collation_for_utf8mb4(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let collation =
        supported_collation(&validated.value, original).map_err(ValidationError::SqlError)?;
    if !matches!(
        collation.name.as_str(),
        "utf8mb4_bin" | "utf8mb4_general_ci" | "utf8mb4_0900_ai_ci"
    ) {
        return Err(ValidationError::SqlError(SqlError::new_f(
            tidb_errno::errcode::ErrInvalidDefaultUTF8MB4Collation,
            tidb_errno::errname::ErrInvalidDefaultUTF8MB4Collation.raw,
            &[],
            &[FormatArg::from(collation.name.as_str())],
        )));
    }
    Ok(Validated {
        value: collation.name,
        truncated: validated.truncated,
    })
}

/// Go keeps this compatibility variable in the integer range [1, 2],
/// then its variable-specific validation closure rejects 1 because the
/// planner no longer has an Analyze v1 path.
pub(super) fn tidb_analyze_version(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value == "1" {
        return Err(ValidationError::Refused(
            "tidb_analyze_version=1 is no longer supported, please set tidb_analyze_version to 2"
                .to_owned(),
        ));
    }
    Ok(validated)
}

/// Go's `tidb_auto_analyze_ratio` accepts values above one but refuses
/// ratios below 0.00001 (within the source's 1e-9 tolerance), keeping
/// the previous setting intact with a bare validation error.
pub(super) fn tidb_auto_analyze_ratio(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let ratio = validated
        .value
        .parse::<f64>()
        .map_err(|_| ValidationError::WrongType)?;
    const MIN_RATIO: f64 = 0.00001;
    const TOLERANCE: f64 = 1e-9;
    if ratio < MIN_RATIO && (ratio - MIN_RATIO).abs() > TOLERANCE {
        return Err(ValidationError::Refused(format!(
            "the value of {} should be greater than or equal to {MIN_RATIO:.6}",
            def.name
        )));
    }
    Ok(validated)
}

/// Go's `tidb_analyze_column_options` Validation accepts only ALL and
/// PREDICATE (case-insensitive); its GLOBAL setter uppercases the
/// accepted text before publication. Keep the stored value canonical
/// and refuse unknown options with the source's bare 1105 error.
pub(super) fn tidb_analyze_column_options(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let choice = validated.value.to_ascii_uppercase();
    if matches!(choice.as_str(), "ALL" | "PREDICATE") {
        return Ok(Validated {
            value: choice,
            truncated: validated.truncated,
        });
    }
    Err(ValidationError::Refused(format!(
        "invalid value for tidb_analyze_column_options, it should be either 'ALL' or 'PREDICATE'"
    )))
}

/// Direct registry users (including Go's validation unit tests) may
/// toggle the process-wide scheduler products without a GLOBAL table.
/// Preserve the source closure's prerequisite check here; SQL writes
/// additionally validate their pending GLOBAL image in `GlobalSysvars`.
pub(super) fn tidb_auto_analyze_concurrency(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if !tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst)
        || !tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
            .load(std::sync::atomic::Ordering::SeqCst)
    {
        let run_auto_analyze =
            tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst);
        let enable_auto_analyze_priority_queue = tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
            .load(std::sync::atomic::Ordering::SeqCst);
        return Err(ValidationError::Refused(format!(
            "cannot set {}: requires both tidb_enable_auto_analyze and tidb_enable_auto_analyze_priority_queue to be true. Current values: tidb_enable_auto_analyze={}, tidb_enable_auto_analyze_priority_queue={}",
            def.name, run_auto_analyze, enable_auto_analyze_priority_queue
        )));
    }
    Ok(validated)
}

/// Pinned Go deprecated the auto-analyze scheduler switch after making
/// the priority queue unconditional. Its bool type validation still
/// runs first, then the variable-specific closure refuses OFF.
pub(super) fn tidb_enable_auto_analyze_priority_queue(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value == "OFF" {
        return Err(ValidationError::Refused(
            "tidb_enable_auto_analyze_priority_queue has been deprecated and TiDB will always use priority queue to schedule auto analyze"
            .to_owned(),
        ));
    }
    Ok(validated)
}

pub(super) fn tidb_workload_repository_dest(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    validate_workload_repository_dest(&validated.value)
        .map(|value| Validated {
            value,
            truncated: validated.truncated,
        })
        .map_err(ValidationError::Refused)
}

/// Go's `tidb_isolation_read_engines` validation: comma-split engines
/// trim, canonicalize case-insensitively to tikv/tiflash/tidb, and an
/// empty or unknown engine is `ErrWrongValueForVar`.
pub(super) fn tidb_isolation_read_engines(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    normalize_isolation_read_engines_value(&validated.value)
        .map(|value| Validated {
            value,
            truncated: validated.truncated,
        })
        .map_err(|()| ValidationError::WrongValue)
}

/// Go's `tidb_allow_fallback_to_tikv` closure accepts an empty list or
/// a comma-separated list containing only TiFlash, trims each token,
/// deduplicates it, and rejects every other engine.
pub(super) fn tidb_allow_fallback_to_tikv(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
    Ok(Validated {
        value: normalized,
        truncated: validated.truncated,
    })
}

/// Go's `tidb_scatter_region` Validation lowercases the requested
/// mode and admits only the empty (off), `table`, and `global` forms.
/// The registry's TypeStr normalization otherwise accepts any text,
/// so keep the source closure here rather than relying on metadata's
/// possible-values list (which is descriptive only).
pub(super) fn tidb_scatter_region(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let lowered = validated.value.to_ascii_lowercase();
    if matches!(lowered.as_str(), "" | "table" | "global") {
        return Ok(Validated {
            value: lowered,
            truncated: validated.truncated,
        });
    }
    Err(ValidationError::Refused(format!(
        "invalid value for '{}', it should be either '', 'table' or 'global'",
        lowered
    )))
}

/// Go's `tidb_enable_table_partition` validation: partitioning is
/// ALWAYS on, so the closure returns `vardef.On` whatever was assigned
/// and only warns when the assignment was `OFF`. The stored value is
/// what `SHOW VARIABLES` reports, so `SET ... = off` followed by `SHOW
/// VARIABLES LIKE 'tidb_enable_table_partition'` reads `ON` (captured
/// through `gorun`, session and global scope alike).
pub(super) fn tidb_enable_table_partition(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    Ok(Validated {
        value: "ON".to_owned(),
        truncated: validated.truncated,
    })
}

/// Go's deprecated planner/DDL compatibility switches are now always
/// enabled. Their Validation closures return `ON` for every boolean
/// spelling; the matching warning is appended by the session write
/// path when the caller explicitly requests `OFF`.
///
/// Shared by `tidb_enable_exchange_partition`, `tidb_enable_new_cost_interface`, `tidb_enable_tiflash_read_for_write_stmt`.
pub(super) fn deprecated_always_on(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    Ok(Validated {
        value: "ON".to_owned(),
        truncated: validated.truncated,
    })
}

/// Go's `tidb_enable_list_partition` validation: list partitioning is
/// also always on, but this one REFUSES anything that is not, with an
/// `errors.Errorf` whose text is the whole error. Captured:
/// `set tidb_enable_list_partition=off` -> `Error 1105 (HY000):
/// tidb_enable_list_partition is now always on, and cannot be turned
/// off`, and the variable stays `ON`.
pub(super) fn tidb_enable_list_partition(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value != "ON" {
        return Err(ValidationError::Refused(
            "tidb_enable_list_partition is now always on, and cannot be turned off".into(),
        ));
    }
    Ok(validated)
}

/// Go's TiDB/TiFlash hash-join-version validation closures accept the
/// two source values case-insensitively, retain the user's spelling,
/// and raise a bare error (1105) for every other value.
///
/// Shared by `tidb_hash_join_version`, `tiflash_hash_join_version`.
pub(super) fn hash_join_versions(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    use tidb_executor_join_joinversion::{HASH_JOIN_VERSION_LEGACY, HASH_JOIN_VERSION_OPTIMIZED};

    let is_valid = validated
        .value
        .eq_ignore_ascii_case(HASH_JOIN_VERSION_LEGACY)
        || validated
            .value
            .eq_ignore_ascii_case(HASH_JOIN_VERSION_OPTIMIZED);
    if !is_valid {
        return Err(ValidationError::Refused(format!(
            "incorrect value: `{original}`. {} options: {HASH_JOIN_VERSION_LEGACY}, {HASH_JOIN_VERSION_OPTIMIZED}",
            def.name
        )));
    }
    Ok(validated)
}

/// Go validates the original `tidb_service_scope` spelling through
/// `pkg/util/naming.Check`, then its SetGlobal hook stores lowercase.
pub(super) fn tidb_service_scope(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    tidb_naming::check(original).map_err(|error| ValidationError::Refused(error.to_string()))?;
    Ok(Validated {
        value: validated.value.to_ascii_lowercase(),
        truncated: validated.truncated,
    })
}

/// Go's `tidb_session_alias` validation: the alias is cut to 64 RUNES
/// (not bytes -- a 65-character Chinese alias loses exactly its last
/// character), and then, since it labels log lines as an identifier,
/// `util.IsInCorrectIdentifierName` strips every TRAILING SPACE off
/// what is left. Both cuts raise Go's `ErrTruncatedWrongValue`.
/// Captured: `set @@tidb_session_alias='abc  '` reads back as `abc` on
/// both `SELECT @@` and `SHOW VARIABLES`.
pub(super) fn tidb_session_alias(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let cut: String = validated.value.chars().take(64).collect();
    let trimmed = cut.trim_end_matches(' ');
    if trimmed.len() != validated.value.len() {
        return Ok(Validated {
            value: trimmed.to_owned(),
            truncated: true,
        });
    }
    Ok(validated)
}

/// Go's `max_allowed_packet` validation (`sysvar.go:2193`): the
/// accepted value is truncated DOWN to a multiple of 1024, and the
/// rounding is reported as `ErrTruncatedWrongValue` (1292) -- which is
/// what the `truncated` flag carries to the statement.
///
/// `MinValue` is 1024, so the type check has already refused anything
/// below one full multiple; the remainder can never take the value to
/// zero here.
///
/// NOT MODELLED (needs a coded error this tier's `VarError` does
/// not carry): Go additionally REFUSES `SET SESSION max_allowed_packet`
/// with `ErrReadOnly` (1621, "SESSION variable 'max_allowed_packet' is
/// read-only. Use SET GLOBAL to assign the value"), and refuses
/// `SET GLOBAL` in starter deployments.
pub(super) fn max_allowed_packet(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if let Ok(parsed) = validated.value.parse::<u64>() {
        let remainder = parsed % 1024;
        if remainder != 0 {
            return Ok(Validated {
                value: (parsed - remainder).to_string(),
                truncated: true,
            });
        }
    }
    Ok(validated)
}

/// Go's `checkCharacterSet` (`varsutil.go:76`) covers every
/// character-set variable (`character_set_filesystem` included). An
/// empty `character_set_results` is the sole exception (it means no
/// result conversion); any other empty value is `ErrWrongValueForVar`
/// (1231) naming the value `NULL`. An unknown name is the plain
/// `Unknown charset <name>` (1105, `charset.GetCharsetInfo`), and a
/// hit stores the canonical charset name.
///
/// Shared by `character_set_results`, `character_set_client`, `character_set_connection`, `character_set_database`, `character_set_filesystem`, `character_set_server`.
pub(super) fn character_set_results(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if def.name == "character_set_results" && validated.value.is_empty() {
        return Ok(validated);
    }
    if validated.value.is_empty() {
        return Err(ValidationError::SqlError(SqlError::new(
            tidb_mysql::errors::errcode::ErrWrongValueForVar,
            &[FormatArg::from(def.name), FormatArg::from("NULL")],
        )));
    }
    let charset = tidb_charset::get_charset_info(&validated.value)
        .map_err(|_| ValidationError::Refused(format!("Unknown charset {}", validated.value)))?;
    Ok(Validated {
        value: charset.name,
        truncated: validated.truncated,
    })
}

/// Go's mpp_exchange_compression_mode Validation (`sysvar.go:3308`):
/// the mode name must parse (`ToExchangeCompressionMode`); a miss is
/// refused with the option list spelled out in the message (a bare
/// errors.Errorf, so it reports as 1105).
pub(super) fn mpp_exchange_compression_mode(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if tidb_vardef::modes::to_exchange_compression_mode(&validated.value).is_none() {
        return Err(ValidationError::Refused(format!(
            "incorrect value: `{original}`. mpp_exchange_compression_mode options: NONE, FAST, HIGH_COMPRESSION, UNSPECIFIED",
            original = original
        )));
    }
    Ok(validated)
}

/// Go's tidb_runtime_filter_type Validation (`sysvar.go:3726`): a
/// comma-separated list whose tokens must each be IN or MIN_MAX in
/// any case; a bad token is refused with the hint message (a bare
/// errors.New, so it reports as 1105).
pub(super) fn tidb_runtime_filter_type(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let ok = validated
        .value
        .split(',')
        .all(|token| matches!(token.trim().to_ascii_uppercase().as_str(), "IN" | "MIN_MAX"));
    if !ok {
        return Err(ValidationError::Refused(format!(
            "incorrect value: {original}. tidb_runtime_filter_type should be sepreated by , such as IN, also we only support IN and MIN_MAX now. "
        )));
    }
    Ok(validated)
}

/// Go's tidb_runtime_filter_mode Validation (`sysvar.go:3741`): only
/// the exact spellings OFF and LOCAL pass (the lookup is
/// case-sensitive); the refusal is the option-list message, 1105.
pub(super) fn tidb_runtime_filter_mode(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if !matches!(validated.value.as_str(), "OFF" | "LOCAL") {
        return Err(ValidationError::Refused(format!(
            "incorrect value: {original}. tidb_runtime_filter_mode options: OFF "
        )));
    }
    Ok(validated)
}

/// Go's init_connect Validation (`sysvar.go:704`): the value must
/// parse as SQL (Go parses with the session's SQL mode; this
/// boundary has no mode, so the stock mode stands in). A parse
/// failure is `ErrWrongTypeForVar` (1232); an empty value parses as
/// zero statements and passes, as in Go.
pub(super) fn init_connect(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if !validated.value.is_empty() && tidb_parser::parse(&validated.value).is_err() {
        return Err(ValidationError::WrongType);
    }
    Ok(validated)
}

/// Go's `tidb_slow_log_rules` hooks: SetSession parses the unbound
/// rules (a ConnID is refused) and GetSession reads back their
/// encoding; SetGlobal parses with ConnIDs allowed and stores the text.
pub(super) fn tidb_slow_log_rules(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if scope == SCOPE_SESSION {
        let encoded = crate::slow_log_rules::session_rules(&validated.value)
            .map_err(ValidationError::Refused)?;
        return Ok(Validated {
            value: encoded,
            truncated: validated.truncated,
        });
    }
    let encoded =
        crate::slow_log_rules::global_rules(&validated.value).map_err(ValidationError::Refused)?;
    Ok(Validated {
        value: encoded,
        truncated: validated.truncated,
    })
}

/// Go's `setPipelinedDmlResourcePolicy` (`sysvar.go`), a SetSession
/// hook: `standard`, `conservative`, or `custom{key=value, ...}` over
/// `concurrency`, `resolve_concurrency` (1..=8192) and
/// `write_throttle_ratio` (0 <= ratio < 1); anything else is
/// `ErrWrongValueForVar`.
pub(super) fn tidb_pipelined_dml_resource_policy(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if scope == SCOPE_SESSION {
        if !pipelined_dml_resource_policy_is_valid(&validated.value) {
            return Err(ValidationError::WrongValue);
        }
        return Ok(validated);
    }
    Ok(validated)
}

/// Go's mpp_version Validation (`sysvar.go:3335`): the value must
/// parse (`ToMppVersion`); a miss is refused with the option list
/// `-1 (unspecified), 0, 1, 2, 3` (a bare errors.Errorf, so it
/// reports as 1105).
pub(super) fn mpp_version(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if tidb_vardef::modes::to_mpp_version(&validated.value).is_none() {
        return Err(ValidationError::Refused(format!(
            "incorrect value: {original}. mpp_version options: -1 (unspecified), 0, 1, 2, 3",
        )));
    }
    Ok(validated)
}

/// The mem-arbitrator Validations (`sysvar.go`, the
/// tidb_mem_arbitrator_* entries): mode lowercases and whitelists
/// disable/standard/priority; wait_averse accepts exactly "0", "1"
/// and "nolimit"; query_reserved accepts "0" or any integer > 1;
/// soft_limit accepts the disable sentinel, `auto`, a positive ratio
/// through 1.0, or an integer byte count greater than 1. All
/// refusals are bare errors, 1105.
pub(super) fn tidb_mem_arbitrator_mode(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let lowered = validated.value.to_ascii_lowercase();
    if matches!(lowered.as_str(), "disable" | "standard" | "priority") {
        return Ok(Validated {
            value: lowered,
            truncated: validated.truncated,
        });
    }
    Err(ValidationError::Refused(format!(
        "incorrect value: {original}. tidb_mem_arbitrator_mode options: disable, standard, priority"
    )))
}

pub(super) fn tidb_mem_arbitrator_wait_averse(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if matches!(validated.value.as_str(), "0" | "1" | "nolimit") {
        return Ok(validated);
    }
    Err(ValidationError::Refused(
        "invalid tidb_mem_arbitrator_wait_averse value; only 0, 1 and nolimit are accepted"
            .to_owned(),
    ))
}

pub(super) fn tidb_mem_arbitrator_query_reserved(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
    Err(ValidationError::Refused(
        "invalid tidb_mem_arbitrator_query_reserved value".to_owned(),
    ))
}

pub(super) fn tidb_mem_arbitrator_soft_limit(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if !validated.value.is_empty() {
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
    Ok(validated)
}

/// Go's tidb_opt_index_join_build_v2 (`sysvar.go:2874`): the
/// planner always uses the v2 path, so a falsy set is refused with
/// the always-enabled message (bare error, 1105) and a truthy set
/// normalizes to ON.
pub(super) fn tidb_opt_index_join_build_v2(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
    Ok(Validated {
        value: "ON".to_owned(),
        truncated: validated.truncated,
    })
}

/// Go's tidb_schema_cache_size Validation (`sysvar.go:3772` via
/// `parseSchemaCacheSize`, `varsutil.go:537`): a byte-size string or
/// plain integer passes through its parsed spelling, sizes below the
/// 64MB floor clamp up to "64MB", sizes above i64::MAX clamp down,
/// and anything unparseable is ErrTruncatedWrongValue (1292). Go
/// additionally appends a 1365 warning on both clamps; this
/// boundary has no warning sink, so only the value is adjusted.
pub(super) fn tidb_schema_cache_size(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
    Err(ValidationError::SqlError(SqlError::new(
        tidb_mysql::errors::errcode::ErrTruncatedWrongValue,
        &[
            FormatArg::from("tidb_schema_cache_size"),
            FormatArg::from(original),
        ],
    )))
}

/// Go's `ValidAnalyzeSkipColumnTypes` (`varsutil.go:501`): tokens are
/// lowercased and trimmed, must each be one of the seven column
/// types, and the joined lower-case list is stored; anything else is
/// `ErrWrongValueForVar` (1231) carrying the original value.
pub(super) fn tidb_analyze_skip_column_types(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
    Ok(Validated {
        value: column_types.join(","),
        truncated: validated.truncated,
    })
}

/// Go's tidb_max_dist_task_nodes Validation (`sysvar.go`): zero is
/// not a legal node count — the message names the legal domain
/// (-1 or [1, 128]); other in-range values pass (a bare
/// errors.New, so it reports as 1105).
pub(super) fn tidb_max_dist_task_nodes(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if let Ok(nodes) = validated.value.parse::<i64>() {
        if nodes == 0 {
            return Err(ValidationError::Refused(
                "max_dist_task_nodes should be -1 or [1, 128]".to_owned(),
            ));
        }
    }
    Ok(validated)
}

/// Go's `tiflash_hashagg_preaggregation_mode` Validation is a
/// case-sensitive lookup over the three TiFlash modes. Preserve the
/// user's accepted spelling and refuse a miss with its option-list
/// error (a bare 1105).
pub(super) fn tiflash_hashagg_preaggregation_mode(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if matches!(
        validated.value.as_str(),
        "force_preagg" | "auto" | "force_streaming"
    ) {
        return Ok(validated);
    }
    Err(ValidationError::Refused(format!(
        "incorrect value: `{original}`. tiflash_hashagg_preaggregation_mode options: force_preagg, auto, force_streaming"
    )))
}

/// Go's tidb_evolve_plan_baselines Validation (`sysvar.go`): ON is
/// refused unless the test-only CheckTableBeforeDrop knob is set,
/// which is false in every deployment (a bare errors.Errorf, 1105).
pub(super) fn tidb_evolve_plan_baselines(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if validated.value == "ON" {
        return Err(ValidationError::Refused(
            "Cannot enable baseline evolution feature, it is not generally available now"
                .to_owned(),
        ));
    }
    Ok(validated)
}

/// Go's `checkIsolationLevel` (`varsutil.go:116`): SERIALIZABLE and
/// READ-UNCOMMITTED are refused with `ErrUnsupportedIsolationLevel`
/// (8048) unless the session's own
/// `tidb_skip_isolation_level_check` is ON, in which case the set
/// proceeds with that warning (no sink on this boundary).
pub(super) fn tx_isolation_one_shot(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if matches!(
        validated.value.as_str(),
        "SERIALIZABLE" | "READ-UNCOMMITTED"
    ) {
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
    Ok(validated)
}

/// Go's `validateReadConsistencyLevel` (`session.go:702`): only
/// `strict` and `weak` in any case pass, stored as typed; everything
/// else is `ErrWrongTypeForVar` (1232).
pub(super) fn tidb_read_consistency(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    let lowered = validated.value.to_ascii_lowercase();
    if lowered != "strict" && lowered != "weak" {
        return Err(ValidationError::WrongType);
    }
    Ok(validated)
}

/// go's ddl-owner sync refuses DISABLING the owner
/// (ErrDDLSetting, 8246) for either scope.
pub(super) fn tidb_enable_ddl(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if original.eq_ignore_ascii_case("OFF") || original == "0" {
        return Err(ValidationError::SqlError(SqlError::new_f(
            tidb_errno::errcode::ErrDDLSetting,
            "Error happened when disabling DDL: can not disable ddl owner when it is the only one tidb instance",
            &[],
            &[],
        )));
    }
    Ok(validated)
}

/// go: the trace-event pipeline is TiDB-X-only; a classic kernel
/// answers bare 1105 for every assignment.
pub(super) fn tidb_trace_event(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    Err(ValidationError::Refused(
        "can only be set for TiDB X kernel".to_owned(),
    ))
}

/// Go `tidb_snapshot`: `parseTSFromNumberOrTime` accepts an unsigned TSO
/// before datetime parsing, and `tidb_read_staleness` must be clear.
pub(super) fn tidb_snapshot(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if !original.is_empty() && original.parse::<u64>().is_err() {
        let zone = tidb_datatype::SessionTimeZone::utc();
        if tidb_datatype::parse_datetime(original, &zone, false, false).is_err() {
            return Err(ValidationError::SqlError(SqlError::new_f(
                tidb_mysql::errors::errcode::ErrTruncatedWrongValue,
                "Incorrect datetime value: '%s'",
                &[],
                &[FormatArg::from(original)],
            )));
        }
    }
    if let Some(lookup) = lookup {
        if !validated.value.is_empty()
            && lookup("tidb_read_staleness").is_some_and(|v| v.parse::<i64>().is_ok_and(|v| v != 0))
        {
            return Err(conflict(
                "tidb_read_staleness should be clear before setting tidb_snapshot",
            ));
        }
    }
    Ok(validated)
}

/// Go `tidb_read_staleness` Validation: `tidb_snapshot` must be clear.
pub(super) fn tidb_read_staleness(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
    if let Some(lookup) = lookup {
        if validated.value.parse::<i64>().is_ok_and(|v| v != 0)
            && lookup("tidb_snapshot").is_some_and(|v| !v.is_empty() && v.parse::<u64>() != Ok(0))
        {
            return Err(conflict(
                "tidb_snapshot should be clear before setting tidb_read_staleness",
            ));
        }
    }
    Ok(validated)
}

pub(super) fn sql_mode(
    def: &SysVarDef,
    validated: Validated,
    original: &str,
    scope: u8,
    lookup: Lookup<'_>,
) -> Result<Validated, ValidationError> {
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
        Err(invalid) => {
            // go's sql_mode Validation embeds the client-rendered 1231 in
            // an uncatalogued error, so the SET surfaces 1105 with the
            // text `ERROR 1231 (42000): Variable 'sql_mode' can't be set
            // to the value of '...'` (the SqlError's own rendering).
            Err(ValidationError::Refused(invalid.sql_error.to_string()))
        }
    }
}
