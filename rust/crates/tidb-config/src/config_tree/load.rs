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
//! - Go decodes into the caller's `Config` (started from `NewConfig`), so
//!   absent keys keep their defaults. Since serde's `Config` default is
//!   `DefaultConfig`, deserializing a partial file yields the same result;
//!   `load_str` replaces `self` with it, matching how the config is always
//!   loaded into a fresh `NewConfig`.
//!
//! The instance-section migration (`sectionMovedToInstance` ->
//! `ErrConfigInstanceSection`) is a following tranche.

use std::collections::BTreeSet;

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
    /// Any other load error (parse failure, a deploy-mode gate, ...).
    Other(String),
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

/// Whether all undecoded items belong to the removed-config set (Go
/// `isAllRemovedConfigItems`).
pub fn is_all_removed_config_items(items: &[String]) -> bool {
    let removed: BTreeSet<&str> = REMOVED_CONFIG.iter().copied().collect();
    items.iter().all(|i| removed.contains(i.as_str()))
}

const MAX_TOKEN_LIMIT: u32 = 1024 * 1024;

impl Config {
    /// Go `Config.Load` (from an already-read config-file string; file I/O
    /// is the caller's, matching how the rewrite reads config text).
    pub fn load_str(&mut self, conf_file: &str, text: &str) -> Result<(), LoadError> {
        let table: toml::Table =
            toml::from_str(text).map_err(|e| LoadError::Other(e.to_string()))?;

        // Decode, collecting unrecognized keys (Go's metaData.Undecoded()).
        let mut undecoded: Vec<String> = Vec::new();
        let de = toml::Deserializer::new(text);
        let parsed: Config = serde_ignored::deserialize(de, |path| {
            undecoded.push(path.to_string());
        })
        .map_err(|e| LoadError::Other(e.to_string()))?;
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

        // Undecoded items -> ErrConfigValidationFailed (the instance-section
        // migration is a following tranche).
        if !undecoded.is_empty() {
            return Err(LoadError::ValidationFailed {
                conf_file: conf_file.to_string(),
                undecoded_items: undecoded,
            });
        }
        Ok(())
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
}
