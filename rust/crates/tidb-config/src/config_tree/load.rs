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

//! Go `pkg/config/config.go`'s `Config.Load` and `RemovedVariableCheck`.
//!
//! Faithful adaptations:
//! - Go's BurntSushi `metaData.IsDefined(section, key)` (was a key present
//!   in the file?) becomes [`is_defined`] over the parsed [`toml::Table`].
//! - Go's `metaData.Undecoded()` (the full list of unrecognized keys)
//!   becomes `serde_ignored`, the Rust equivalent — it records every
//!   ignored key path while still deserializing normally.
//! - Go decodes into the caller's existing `Config`, so absent keys retain
//!   their current values. `load_str` recursively overlays the input TOML
//!   on a serialized snapshot to preserve the same behavior.
//!
//! The instance-section migration (`sectionMovedToInstance` ->
//! `ErrConfigInstanceSection`) reports options that were relocated into the
//! `[instance]` section: an old option that collides with its new
//! `[instance]` name is a *conflict*; one present only under its old name is
//! *deprecated*. Either produces `LoadError::InstanceSection`.

use std::collections::{BTreeMap, BTreeSet};

use super::config::Config;
use crate::deploymode::Mode;

/// A config-load error (Go's `ErrConfigValidationFailed` and plain errors).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LoadError {
    /// The config file contained unrecognized options (Go
    /// `ErrConfigValidationFailed`). Callers may downgrade this to a
    /// warning, as `InitializeConfig` does.
    ValidationFailed {
        /// The config file path.
        conf_file: String,
        /// The unrecognized item paths.
        undecoded_items: Vec<String>,
    },
    /// Options were relocated into the `[instance]` section (Go
    /// `ErrConfigInstanceSection`). Callers downgrade this to a warning.
    InstanceSection {
        /// The config file path.
        conf_file: String,
        /// Old options that also appear under their new `[instance]` name.
        conflict: Vec<InstanceConfigSection>,
        /// Old options present only under their old name.
        deprecated: Vec<InstanceConfigSection>,
    },
    /// Any other load error (parse failure, a deploy-mode gate, ...).
    Other(String),
}

/// A group of relocated options from one source section (Go
/// `InstanceConfigSection`). `section_name` is empty for top-level options.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InstanceConfigSection {
    /// The originating section name (empty = top level).
    pub section_name: String,
    /// old-option-name -> new `[instance]` option name.
    pub name_mappings: BTreeMap<String, String>,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::ValidationFailed {
                conf_file,
                undecoded_items,
            } => write!(
                f,
                "config file {conf_file} contained invalid configuration options: {}; check \
                 TiDB manual to make sure this option has not been deprecated and removed from \
                 your TiDB version if the option does not appear to be a typo",
                undecoded_items.join(", ")
            ),
            LoadError::InstanceSection {
                conflict,
                deprecated,
                ..
            } => {
                if !conflict.is_empty() {
                    f.write_str(
                        "Conflict configuration options exists on both [instance] section and \
                         some other sections. ",
                    )?;
                }
                if !deprecated.is_empty() {
                    f.write_str(
                        "Some configuration options should be moved to [instance] section. ",
                    )?;
                }
                f.write_str("Please use the latter config options in [instance] instead: ")?;
                for section in conflict.iter().chain(deprecated.iter()) {
                    for (old_name, new_name) in &section.name_mappings {
                        write!(f, " ({old_name}, {new_name})")?;
                    }
                }
                f.write_str(".")
            }
            LoadError::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for LoadError {}

/// Go `metaData.IsDefined`: whether the dotted key path is present in the
/// parsed TOML table.
pub fn is_defined(table: &toml::Table, keys: &[&str]) -> bool {
    let mut cur = table;
    for (i, k) in keys.iter().enumerate() {
        match cur.get(*k) {
            None => return false,
            Some(v) => {
                if i == keys.len() - 1 {
                    return true;
                }
                match v.as_table() {
                    Some(t) => cur = t,
                    None => return false,
                }
            }
        }
    }
    true
}

// Config items no longer supported (Go `removedConfig`).
const REMOVED_CONFIG: &[&str] = &[
    "pessimistic-txn.ttl",
    "pessimistic-txn.enable",
    "log.file.log-rotate",
    "log.log-slow-query",
    "txn-local-latches",
    "txn-local-latches.enabled",
    "txn-local-latches.capacity",
    "performance.max-memory",
    "max-txn-time-use",
    "experimental.allow-auto-random",
    "enable-redact-log",
    "enable-streaming",
    "performance.mem-profile-interval",
    "security.require-secure-transport",
    "lower-case-table-names",
    "stmt-summary",
    "stmt-summary.enable",
    "stmt-summary.enable-internal-query",
    "stmt-summary.max-stmt-count",
    "stmt-summary.max-sql-length",
    "stmt-summary.refresh-interval",
    "stmt-summary.history-size",
    "enable-batch-dml",
    "mem-quota-query",
    "log.query-log-max-len",
    "performance.committer-concurrency",
    "experimental.enable-global-kill",
    "performance.run-auto-analyze",
    "prepared-plan-cache.enabled",
    "prepared-plan-cache.capacity",
    "prepared-plan-cache.memory-guard-ratio",
    "oom-action",
    "check-mb4-value-in-utf8",
    "enable-collect-execution-info",
    "log.enable-slow-log",
    "log.slow-threshold",
    "log.record-plan-in-slow-log",
    "log.expensive-threshold",
    "performance.force-priority",
    "performance.memory-usage-alarm-ratio",
    "plugin.load",
    "plugin.dir",
    "performance.feedback-probability",
    "performance.query-feedback-limit",
    "oom-use-tmp-storage",
    "max-server-connections",
    "run-ddl",
    "instance.tidb_memory_usage_alarm_ratio",
    "enable-global-index",
];

/// Config items hidden from the JSON dump though still accepted (Go
/// `hideConfig`).
const HIDE_CONFIG: &[&str] = &["performance.index-usage-sync-lease"];

/// Go `ContainHiddenConfig`.
pub fn contain_hidden_config(value: &str) -> bool {
    let value = value.to_lowercase();
    HIDE_CONFIG
        .iter()
        .chain(REMOVED_CONFIG.iter())
        .any(|hidden| value.contains(hidden))
}

/// Whether all undecoded items belong to the removed-config set (Go
/// `isAllRemovedConfigItems`).
pub fn is_all_removed_config_items(items: &[String]) -> bool {
    let removed: BTreeSet<&str> = REMOVED_CONFIG.iter().copied().collect();
    items.iter().all(|i| removed.contains(i.as_str()))
}

const MAX_TOKEN_LIMIT: u32 = 1024 * 1024;

/// Options relocated into the `[instance]` section (Go
/// `sectionMovedToInstance`): `(source_section, &[(old_name, new_name)])`;
/// an empty source section means the option lived at the top level.
const SECTION_MOVED_TO_INSTANCE: &[(&str, &[(&str, &str)])] = &[
    (
        "",
        &[
            ("check-mb4-value-in-utf8", "tidb_check_mb4_value_in_utf8"),
            (
                "enable-collect-execution-info",
                "tidb_enable_collect_execution_info",
            ),
            ("max-server-connections", "max_connections"),
            ("run-ddl", "tidb_enable_ddl"),
        ],
    ),
    (
        "log",
        &[
            ("enable-slow-log", "tidb_enable_slow_log"),
            ("slow-threshold", "tidb_slow_log_threshold"),
            ("record-plan-in-slow-log", "tidb_record_plan_in_slow_log"),
        ],
    ),
    (
        "performance",
        &[
            ("force-priority", "tidb_force_priority"),
            ("memory-usage-alarm-ratio", "tidb_memory_usage_alarm_ratio"),
        ],
    ),
    ("plugin", &[("load", "plugin_load"), ("dir", "plugin_dir")]),
];

/// Go's instance-section migration scan: classify each relocated option that
/// is present in the file as either a conflict (its new `[instance]` name is
/// also set) or deprecated (only the old name is set).
fn instance_section_migration(
    table: &toml::Table,
) -> (Vec<InstanceConfigSection>, Vec<InstanceConfigSection>) {
    let mut conflict = Vec::new();
    let mut deprecated = Vec::new();
    for (section, mappings) in SECTION_MOVED_TO_INSTANCE {
        let mut conflict_map = BTreeMap::new();
        let mut deprecated_map = BTreeMap::new();
        for (old_name, new_name) in *mappings {
            let old_defined = if section.is_empty() {
                is_defined(table, &[old_name])
            } else {
                is_defined(table, &[section, old_name])
            };
            if old_defined {
                if is_defined(table, &["instance", new_name]) {
                    conflict_map.insert((*old_name).to_string(), (*new_name).to_string());
                } else {
                    deprecated_map.insert((*old_name).to_string(), (*new_name).to_string());
                }
            }
        }
        if !conflict_map.is_empty() {
            conflict.push(InstanceConfigSection {
                section_name: (*section).to_string(),
                name_mappings: conflict_map,
            });
        }
        if !deprecated_map.is_empty() {
            deprecated.push(InstanceConfigSection {
                section_name: (*section).to_string(),
                name_mappings: deprecated_map,
            });
        }
    }
    (conflict, deprecated)
}

fn format_go_toml_type_error(
    text: &str,
    table: &toml::Table,
    error: &toml::de::Error,
) -> Option<String> {
    let rendered = error.to_string();
    let destination_type = if rendered.contains("expected a boolean") {
        "boolean"
    } else {
        return None;
    };
    let span = error.span()?;
    let line_index = text[..span.start]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count();
    let lines: Vec<&str> = text.lines().collect();
    let line = *lines.get(line_index)?;
    let key = line.split_once('=')?.0.trim();

    let section = lines[..line_index]
        .iter()
        .rev()
        .map(|line| line.trim())
        .find(|line| line.starts_with('[') && line.ends_with(']'))?
        .trim_matches(['[', ']']);
    let mut current = table;
    for component in section.split('.') {
        current = current.get(component)?.as_table()?;
    }
    let source_type = match current.get(key)? {
        toml::Value::Integer(_) => "int64",
        toml::Value::Float(_) => "float64",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::String(_) => "string",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    };

    Some(format!(
        "toml: line {} (last key \"{section}.{key}\"): incompatible types: TOML value has type {source_type}; destination has type {destination_type}",
        line_index + 1
    ))
}

fn merge_toml_value(base: &mut toml::Value, overlay: toml::Value) {
    if let toml::Value::Table(overlay_table) = overlay {
        if let Some(base_table) = base.as_table_mut() {
            for (key, value) in overlay_table {
                if let Some(existing) = base_table.get_mut(&key) {
                    merge_toml_value(existing, value);
                } else {
                    base_table.insert(key, value);
                }
            }
            return;
        }
        *base = toml::Value::Table(overlay_table);
    } else {
        *base = overlay;
    }
}

impl Config {
    /// Go `Config.Load` (from an already-read config-file string; file I/O
    /// is the caller's, matching how the rewrite reads config text).
    pub fn load_str(&mut self, conf_file: &str, text: &str) -> Result<(), LoadError> {
        let table: toml::Table =
            toml::from_str(text).map_err(|e| LoadError::Other(e.to_string()))?;

        // Decode, collecting unrecognized keys (Go's metaData.Undecoded()).
        let mut undecoded: Vec<String> = Vec::new();
        let de = toml::Deserializer::new(text);
        let _: Config = serde_ignored::deserialize(de, |path| {
            undecoded.push(path.to_string());
        })
        .map_err(|error| {
            LoadError::Other(
                format_go_toml_type_error(text, &table, &error)
                    .unwrap_or_else(|| error.to_string()),
            )
        })?;

        let txn_local_latches = self.txn_local_latches;
        let mem_profile_interval = self.performance.mem_profile_interval.clone();
        let keyspace_observability_values = self.keyspace_observability_values.clone();
        let mut merged =
            toml::Value::try_from(&*self).map_err(|error| LoadError::Other(error.to_string()))?;
        let mut overlay = table.clone();
        if let Some(log) = overlay.remove("Log") {
            overlay.insert("log".to_owned(), log);
        }
        merge_toml_value(&mut merged, toml::Value::Table(overlay));
        let mut parsed: Config = merged
            .try_into()
            .map_err(|error: toml::de::Error| LoadError::Other(error.to_string()))?;
        parsed.txn_local_latches = txn_local_latches;
        parsed.performance.mem_profile_interval = mem_profile_interval;
        parsed.keyspace_observability_values = keyspace_observability_values;
        *self = parsed;

        if !crate::kerneltype::is_next_gen() && is_defined(&table, &["deploy-mode"]) {
            return Err(LoadError::Other(
                "deploy-mode can only be configured for nextgen TiDB".into(),
            ));
        }
        let dxf_defined = is_defined(&table, &["dxf-resource-limit"]);
        if !dxf_defined && self.dxf_resource_limit == 0 {
            self.dxf_resource_limit = 100; // DefDXFResourceLimit
        }
        if dxf_defined && self.deploy_mode != Mode::PremiumReserved {
            return Err(LoadError::Other(
                "dxf-resource-limit can only be configured when deploy-mode is premium_reserved"
                    .into(),
            ));
        }
        if is_defined(&table, &["error-msg-extension"]) && self.deploy_mode != Mode::Starter {
            return Err(LoadError::Other(
                "error-msg-extension can only be configured when deploy-mode is starter".into(),
            ));
        }
        if is_defined(&table, &["external-workload"]) && self.deploy_mode != Mode::Starter {
            return Err(LoadError::Other(
                "external-workload can only be configured when deploy-mode is starter".into(),
            ));
        }
        if self.deploy_mode == Mode::Starter
            && !is_defined(&table, &["standby", "enable-zero-backend"])
        {
            self.standby.enable_zero_backend = true;
        }
        if self.token_limit == 0 {
            self.token_limit = 1000;
        } else if self.token_limit > MAX_TOKEN_LIMIT {
            self.token_limit = MAX_TOKEN_LIMIT;
        }

        // Undecoded items -> ErrConfigValidationFailed. The instance-section
        // migration below takes precedence (Go overwrites `err`).
        let mut err: Option<LoadError> = if undecoded.is_empty() {
            None
        } else {
            Some(LoadError::ValidationFailed {
                conf_file: conf_file.to_string(),
                undecoded_items: undecoded,
            })
        };

        let (conflict, deprecated) = instance_section_migration(&table);
        if !conflict.is_empty() || !deprecated.is_empty() {
            err = Some(LoadError::InstanceSection {
                conf_file: conf_file.to_string(),
                conflict,
                deprecated,
            });
        }

        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Go `RemovedVariableCheck`: errors listing any removed items present.
    pub fn removed_variable_check(&self, text: &str) -> Result<(), String> {
        let table: toml::Table = toml::from_str(text).map_err(|e| e.to_string())?;
        let mut removed: Vec<&str> = Vec::new();
        for item in REMOVED_CONFIG {
            let parts: Vec<&str> = item.split('.').collect();
            let present = match parts.len() {
                2 => is_defined(&table, &[parts[0], parts[1]]),
                1 => is_defined(&table, &[parts[0]]),
                _ => false,
            };
            if present {
                removed.push(item);
            }
        }
        if !removed.is_empty() {
            removed.sort();
            return Err(format!(
                "The following configuration options are no longer supported in this version of \
                 TiDB. Check the release notes for more information: {}",
                removed.join(", ")
            ));
        }
        Ok(())
    }

    /// Go `GetJSONConfig`: the config as tab-indented JSON with the removed
    /// and hidden items stripped (used by `SHOW CONFIG` / the status API).
    pub fn get_json_config(&self) -> Result<String, String> {
        let mut value = serde_json::to_value(self).map_err(|e| e.to_string())?;
        let root = value
            .as_object_mut()
            .ok_or_else(|| "config did not serialize to a JSON object".to_string())?;

        // Go strips removedConfig then hideConfig; deletes on absent paths
        // are no-ops, matching Go's walk that breaks on a missing key.
        for path in REMOVED_CONFIG.iter().chain(HIDE_CONFIG.iter()) {
            delete_json_path(root, path);
        }

        // Go re-marshals with json.Indent(..., "", "\t"): tab indentation.
        let mut buf = Vec::new();
        let fmt = serde_json::ser::PrettyFormatter::with_indent(b"\t");
        let mut ser = serde_json::Serializer::with_formatter(&mut buf, fmt);
        serde::Serialize::serialize(&value, &mut ser).map_err(|e| e.to_string())?;
        String::from_utf8(buf).map_err(|e| e.to_string())
    }
}

/// Delete a dotted path from a JSON object, mirroring Go's walk in
/// `GetJSONConfig`: descend through object keys, remove the final key, and
/// stop early if any intermediate key is missing or not an object.
fn delete_json_path(root: &mut serde_json::Map<String, serde_json::Value>, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut cur = root;
    for (i, key) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            cur.remove(*key);
            return;
        }
        match cur.get_mut(*key) {
            Some(serde_json::Value::Object(m)) => cur = m,
            _ => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_tree::new_config;

    // Go TestTokenLimit.
    #[test]
    fn token_limit() {
        for (input, expected) in [(0u32, 1000u32), (99999999999u64 as u32, MAX_TOKEN_LIMIT)] {
            let mut c = new_config();
            let text = format!("token-limit = {input}\n");
            c.load_str("c.toml", &text).unwrap();
            assert_eq!(c.token_limit, expected);
        }
        // A too-large literal that fits u32 but exceeds the max.
        let mut c = new_config();
        c.load_str("c.toml", "token-limit = 2000000\n").unwrap();
        assert_eq!(c.token_limit, MAX_TOKEN_LIMIT);
    }

    // Go TestDeployModeConfig (classic-kernel path; the Rust default build
    // is the Classic kernel).
    #[test]
    fn deploy_mode_classic() {
        if crate::kerneltype::is_next_gen() {
            return;
        }
        // dxf-resource-limit with default (premium) deploy-mode -> error.
        let mut c = new_config();
        let err = c.load_str("c.toml", "dxf-resource-limit = 30").unwrap_err();
        assert!(err.to_string().contains(
            "dxf-resource-limit can only be configured when deploy-mode is premium_reserved"
        ));

        // deploy-mode set on a classic kernel -> error.
        let mut c = new_config();
        let err = c
            .load_str("c.toml", r#"deploy-mode = "premium""#)
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("deploy-mode can only be configured for nextgen TiDB"));

        // premium_reserved on a classic kernel -> Valid() rejects it.
        let mut c = new_config();
        c.deploy_mode = Mode::PremiumReserved;
        assert!(c
            .valid()
            .unwrap_err()
            .contains("deploy-mode can only be configured for nextgen TiDB"));
    }

    /// Go TestDeployModeConfig's opening defaults block, shared by both
    /// kernels.
    #[test]
    fn deploy_mode_defaults_and_invalid_mode() {
        let mut conf = new_config();
        assert_eq!(conf.deploy_mode, Mode::Premium);
        assert_eq!(
            conf.dxf_resource_limit,
            crate::config_tree::config::DEF_DXF_RESOURCE_LIMIT
        );
        conf.valid().unwrap();

        conf.deploy_mode = Mode::Unknown(100);
        assert!(conf
            .valid()
            .unwrap_err()
            .contains("invalid deploy-mode=unknown(100)"));

        conf.deploy_mode = Mode::Premium;
        conf.max_allowed_packet = 0;
        conf.valid().unwrap();
    }

    // Go TestRemovedVariableCheck (a representative subset; the Go test
    // enumerates the full removed set).
    #[test]
    fn removed_variable_check() {
        let c = new_config();
        // Top-level removed item.
        let err = c
            .removed_variable_check("oom-action = \"cancel\"\n")
            .unwrap_err();
        assert!(err.contains("no longer supported"));
        assert!(err.contains("oom-action"));

        // Nested removed item.
        let err = c
            .removed_variable_check("[performance]\nmax-memory = 1\n")
            .unwrap_err();
        assert!(err.contains("performance.max-memory"));

        // Deterministic sort when several are present.
        let err = c
            .removed_variable_check("oom-action = \"cancel\"\nenable-batch-dml = true\n")
            .unwrap_err();
        let idx_batch = err.find("enable-batch-dml").unwrap();
        let idx_oom = err.find("oom-action").unwrap();
        assert!(idx_batch < idx_oom, "sorted order: {err}");

        // A config with no removed items passes.
        c.removed_variable_check("port = 4000\n").unwrap();
    }

    // Undecoded (unknown) keys produce a validation error listing them.
    #[test]
    fn undecoded_items() {
        let mut c = new_config();
        let err = c
            .load_str("c.toml", "totally-unknown-key = 5\nport = 4000\n")
            .unwrap_err();
        match err {
            LoadError::ValidationFailed {
                undecoded_items, ..
            } => {
                assert!(undecoded_items.iter().any(|i| i == "totally-unknown-key"));
            }
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
        assert!(is_all_removed_config_items(&["oom-action".to_string()]));
        assert!(!is_all_removed_config_items(&[
            "totally-unknown-key".to_string()
        ]));
    }

    // Helper: pull the (conflict, deprecated) sections out of the error,
    // keyed by section name, for order-independent assertions.
    fn sections_by_name(
        v: &[InstanceConfigSection],
    ) -> std::collections::BTreeMap<&str, &BTreeMap<String, String>> {
        v.iter()
            .map(|s| (s.section_name.as_str(), &s.name_mappings))
            .collect()
    }

    // Go TestConflictInstanceConfig: an old option and its new [instance]
    // name both set -> a conflict; the option keeps both values.
    #[test]
    fn conflict_instance_config() {
        let mut c = new_config();
        let text = "check-mb4-value-in-utf8 = true \nrun-ddl = true \n\
             [log] \nenable-slow-log = true \n\
             [performance] \nforce-priority = \"NO_PRIORITY\"\n\
             [instance] \ntidb_check_mb4_value_in_utf8 = false \ntidb_enable_slow_log = false \n\
             tidb_force_priority = \"LOW_PRIORITY\"\ntidb_enable_ddl = false\n\
             tidb_enable_stats_owner = false";
        let err = c.load_str("c.toml", text).unwrap_err();
        assert!(err.to_string().contains(
            "Conflict configuration options exists on both [instance] section and some other \
             sections."
        ));
        match err {
            LoadError::InstanceSection {
                conflict,
                deprecated,
                ..
            } => {
                assert!(deprecated.is_empty(), "no deprecated: {deprecated:?}");
                let by = sections_by_name(&conflict);
                assert_eq!(
                    by[""]["check-mb4-value-in-utf8"],
                    "tidb_check_mb4_value_in_utf8"
                );
                assert_eq!(by[""]["run-ddl"], "tidb_enable_ddl");
                assert_eq!(by["log"]["enable-slow-log"], "tidb_enable_slow_log");
                assert_eq!(by["performance"]["force-priority"], "tidb_force_priority");
            }
            other => panic!("expected InstanceSection, got {other:?}"),
        }
    }

    // Go TestDeprecatedConfig: old options present without their new
    // [instance] name -> deprecated (should be moved to [instance]).
    #[test]
    fn deprecated_config() {
        let mut c = new_config();
        let text = "enable-collect-execution-info = false \nrun-ddl = false \n\
             [plugin] \ndir=\"/plugin-path\" \nload=\"audit-1,whitelist-1\" \n\
             [log] \nslow-threshold = 100 \n\
             [performance] \nmemory-usage-alarm-ratio = 0.5";
        let err = c.load_str("c.toml", text).unwrap_err();
        assert!(err
            .to_string()
            .contains("Some configuration options should be moved to [instance] section."));
        match err {
            LoadError::InstanceSection {
                conflict,
                deprecated,
                ..
            } => {
                assert!(conflict.is_empty(), "no conflict: {conflict:?}");
                let by = sections_by_name(&deprecated);
                assert_eq!(
                    by[""]["enable-collect-execution-info"],
                    "tidb_enable_collect_execution_info"
                );
                assert_eq!(by[""]["run-ddl"], "tidb_enable_ddl");
                assert_eq!(by["log"]["slow-threshold"], "tidb_slow_log_threshold");
                assert_eq!(
                    by["performance"]["memory-usage-alarm-ratio"],
                    "tidb_memory_usage_alarm_ratio"
                );
                assert_eq!(by["plugin"]["load"], "plugin_load");
                assert_eq!(by["plugin"]["dir"], "plugin_dir");
            }
            other => panic!("expected InstanceSection, got {other:?}"),
        }
    }

    // Go TestGetJSONConfig: hidden/removed items are stripped, live ones stay.
    #[test]
    fn get_json_config() {
        let c = new_config();
        let json = c.get_json_config().unwrap();
        // Hidden and removed items must not appear.
        for absent in [
            "index-usage-sync-lease",
            "enable-batch-dml",
            "mem-quota-query",
            "query-log-max-len",
            "oom-action",
        ] {
            assert!(!json.contains(absent), "should not contain {absent}");
        }
        // Live items remain.
        assert!(
            json.contains("stmt-count-limit"),
            "missing stmt-count-limit"
        );
        assert!(json.contains("rpc-metrics"), "missing rpc-metrics");
        // Tab-indented (Go json.Indent with "\t").
        assert!(json.contains("\n\t"), "expected tab indentation");
    }

    // Go TestInvalidConfigWithDeprecatedConfig.
    #[test]
    fn invalid_config_with_deprecated_config() {
        let text = "\n[log]\nslow-threshold = 1000\n[performance]\nenforce-mpp = 1\n";
        let mut config = Config::default();
        let error = config.load_str("config.toml", text).unwrap_err();
        assert_eq!(
            error.to_string(),
            "toml: line 5 (last key \"performance.enforce-mpp\"): incompatible types: TOML value has type int64; destination has type boolean"
        );
    }

    // The load portion of Go TestErrorMessageExtensionConfig.
    #[test]
    fn error_message_extension_config_load() {
        use crate::config_tree::ErrorMessageExtension;

        let text = r#"
error-msg-extension = [
  { pattern = "^Access denied for user '.+'@'.+' \\(using password: (YES|NO)\\)$", suffix = "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details" },
  { pattern = "^require_secure_transport can not be set to ON with SEM\\(security enhanced mode\\) enabled$", suffix = "see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details" },
  { pattern = "^sleep\\(\\) argument is greater than [0-9]+$", suffix = "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details" },
  { pattern = "^[A-Z ]+ command denied to user '[^']+'@'[^']+' for table '[^']+'$", suffix = "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details" },
  { pattern = "^Access denied; you need \\(at least one of\\) the RESTRICTED_VARIABLES_ADMIN privilege\\(s\\) for this operation$", suffix = "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details" },
  { pattern = "^Feature '.+' is not supported when security enhanced mode is enabled$", suffix = "see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details" },
]
"#;
        let expected = vec![
            ErrorMessageExtension {
                pattern: r"^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$"
                    .to_owned(),
                suffix: "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details".to_owned(),
            },
            ErrorMessageExtension {
                pattern: r"^require_secure_transport can not be set to ON with SEM\(security enhanced mode\) enabled$".to_owned(),
                suffix: "see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details".to_owned(),
            },
            ErrorMessageExtension {
                pattern: r"^sleep\(\) argument is greater than [0-9]+$".to_owned(),
                suffix: "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details".to_owned(),
            },
            ErrorMessageExtension {
                pattern: "^[A-Z ]+ command denied to user '[^']+'@'[^']+' for table '[^']+'$".to_owned(),
                suffix: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details".to_owned(),
            },
            ErrorMessageExtension {
                pattern: r"^Access denied; you need \(at least one of\) the RESTRICTED_VARIABLES_ADMIN privilege\(s\) for this operation$".to_owned(),
                suffix: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details".to_owned(),
            },
            ErrorMessageExtension {
                pattern: "^Feature '.+' is not supported when security enhanced mode is enabled$"
                    .to_owned(),
                suffix: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details".to_owned(),
            },
        ];

        let mut config = new_config();
        config.deploy_mode = Mode::Starter;
        config.load_str("config.toml", text).unwrap();
        assert_eq!(config.deploy_mode, Mode::Starter);
        assert_eq!(config.error_message_extensions, expected);
        assert!(new_config().error_message_extensions.is_empty());
    }

    // Go TestConfigExample.
    #[test]
    fn test_config_example() {
        const SAMPLE: &str = include_str!("../../../../../pkg/config/config.toml.example");

        fn check_keys(value: &toml::Value) {
            match value {
                toml::Value::Table(table) => {
                    for (key, value) in table {
                        assert!(!contain_hidden_config(key), "{key} should be hidden");
                        check_keys(value);
                    }
                }
                toml::Value::Array(values) => {
                    for value in values {
                        check_keys(value);
                    }
                }
                _ => {}
            }
        }

        let value: toml::Value = toml::from_str(SAMPLE).unwrap();
        let _: Config = toml::from_str(SAMPLE).unwrap();
        check_keys(&value);
    }

    // A valid partial config loads and keeps defaults.
    #[test]
    fn load_partial() {
        let mut c = new_config();
        c.load_str("c.toml", "[performance]\ncross-join = false\n")
            .unwrap();
        assert!(!c.performance.cross_join);
        assert_eq!(c.host, "0.0.0.0");
        assert_eq!(c.token_limit, 1000);
    }

    #[test]
    fn load_partial_preserves_resolved_keyspace_observability() {
        let mut config: Config = toml::from_str(
            r#"
[[keyspace-observability.fields]]
source = "meta_a"
metric-label = "keyspace_meta_label_a"
slow-log-field = "Keyspace_meta_slow_a"
stmt-log-field = "stmt_meta_a"
"#,
        )
        .unwrap();
        config
            .resolve_keyspace_observability(&std::collections::HashMap::from([(
                "meta_a".to_owned(),
                "value_a".to_owned(),
            )]))
            .unwrap();

        config.load_str("c.toml", "port = 4100\n").unwrap();

        assert_eq!(config.port, 4100);
        assert_eq!(
            config.get_keyspace_observability_metric_labels(),
            &std::collections::HashMap::from([(
                "keyspace_meta_label_a".to_owned(),
                "value_a".to_owned()
            )])
        );
        assert_eq!(
            config.get_keyspace_observability_slow_log_fields(),
            [
                crate::keyspace_observability::KeyspaceObservabilityLogField {
                    name: "Keyspace_meta_slow_a".to_owned(),
                    value: "value_a".to_owned(),
                }
            ]
        );
        assert_eq!(
            config.get_keyspace_observability_stmt_log_fields(),
            &std::collections::HashMap::from([("stmt_meta_a".to_owned(), "value_a".to_owned())])
        );
    }
}
