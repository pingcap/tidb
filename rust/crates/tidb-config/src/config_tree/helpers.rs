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

//! Self-contained pieces of Go `pkg/config/config.go` and
//! `config_util.go`: the error-message-extension preparation, the `CSE`
//! and `TrxSummary` sub-sections, the `max_allowed_packet` validity rule,
//! `FlattenConfigItems`, and `MergeConfigItems`.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::configtypes::Duration;

use super::config::Config;

/// max_allowed_packet must be in `[1024, 1<<30]` and a multiple of 1024.
pub const MAX_ALLOWED_PACKET_UNIT: u64 = 1024;
/// Minimum max_allowed_packet (Go `minMaxAllowedPacket`).
pub const MIN_MAX_ALLOWED_PACKET: u64 = MAX_ALLOWED_PACKET_UNIT;
/// Maximum max_allowed_packet (Go `maxOfMaxAllowedPacket`).
pub const MAX_OF_MAX_ALLOWED_PACKET: u64 = 1 << 30;
/// Default max_allowed_packet (Go `DefMaxAllowedPacket`).
pub const DEF_MAX_ALLOWED_PACKET: u64 = 64 << 20;

/// Go `validMaxAllowedPacket`.
pub fn valid_max_allowed_packet(v: u64) -> bool {
    (MIN_MAX_ALLOWED_PACKET..=MAX_OF_MAX_ALLOWED_PACKET).contains(&v)
        && v.is_multiple_of(MAX_ALLOWED_PACKET_UNIT)
}

/// Appends configured suffixes to matching user-facing errors (Go
/// `ErrorMessageExtension`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ErrorMessageExtension {
    /// The (anchored) pattern to match against SQL errors.
    #[serde(rename = "pattern")]
    pub pattern: String,
    /// The suffix appended to matching errors.
    #[serde(rename = "suffix")]
    pub suffix: String,
}

/// Prepares/validates error-message extensions (Go
/// `prepareErrorMessageExtensions`): compiles each pattern, drops or errors
/// on invalid ones, then sorts by descending pattern length, then pattern,
/// then suffix. Returns `(prepared, first_error)`.
pub fn prepare_error_message_extensions(
    extensions: &[ErrorMessageExtension],
    ignore_invalid: bool,
) -> Result<(Vec<ErrorMessageExtension>, Option<String>), String> {
    let mut prepared: Vec<ErrorMessageExtension> = Vec::with_capacity(extensions.len());
    let mut first_err: Option<String> = None;
    for ext in extensions {
        if ext.pattern.trim().is_empty() {
            let msg = "empty error-msg-extension pattern".to_string();
            if ignore_invalid {
                first_err.get_or_insert(msg);
                continue;
            }
            return Err(msg);
        }
        // Go uses RE2 (regex crate is also RE2-based): operator patterns
        // cannot cause catastrophic backtracking.
        if regex::Regex::new(&ext.pattern).is_err() {
            let msg = format!("invalid error-msg-extension regexp {:?}", ext.pattern);
            if ignore_invalid {
                first_err.get_or_insert(msg);
                continue;
            }
            return Err(msg);
        }
        prepared.push(ext.clone());
    }
    prepared.sort_by(|a, b| {
        if a.pattern.len() != b.pattern.len() {
            return b.pattern.len().cmp(&a.pattern.len());
        }
        if a.pattern != b.pattern {
            return a.pattern.cmp(&b.pattern);
        }
        a.suffix.cmp(&b.suffix)
    });
    Ok((prepared, first_err))
}

/// Cloud storage engine config (Go `CSE`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Cse {
    /// The columnar store type: `tiflash`, `columnar`, or `both`.
    #[serde(rename = "columnar-store-type")]
    pub columnar_store_type: String,
    /// Columnar collect timeout (nanoseconds, Go `time.Duration`).
    #[serde(rename = "columnar-collect-timeout")]
    pub columnar_collect_timeout: i64,
}

impl Cse {
    /// Go `IsTiFlashEnabled`.
    pub fn is_tiflash_enabled(&self) -> bool {
        self.columnar_store_type == "tiflash" || self.columnar_store_type == "both"
    }
    /// Go `IsColumnarStoreEnabled`.
    pub fn is_columnar_store_enabled(&self) -> bool {
        self.columnar_store_type == "columnar" || self.columnar_store_type == "both"
    }
    /// Go `Valid`.
    pub fn valid(&self) -> bool {
        matches!(
            self.columnar_store_type.as_str(),
            "tiflash" | "columnar" | "both"
        )
    }
    /// The default CSE (Go `defaultConf.CSE`).
    pub fn default_config() -> Cse {
        Cse {
            columnar_store_type: "tiflash".into(),
            columnar_collect_timeout: Duration(5 * 1_000_000_000).0,
        }
    }
}

/// Transaction-summary collector config (Go `TrxSummary`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TrxSummary {
    /// How many transaction summaries each node keeps.
    #[serde(rename = "transaction-summary-capacity")]
    pub transaction_summary_capacity: u32,
    /// Min execution duration to be recorded, in seconds-ish units.
    #[serde(rename = "transaction-id-digest-min-duration")]
    pub transaction_id_digest_min_duration: u32,
}

impl Default for TrxSummary {
    // Go `DefaultTrxSummary`.
    fn default() -> Self {
        TrxSummary {
            transaction_summary_capacity: 500,
            transaction_id_digest_min_duration: 2147483647,
        }
    }
}

impl TrxSummary {
    /// Go `Valid`.
    pub fn valid(&self) -> Result<(), String> {
        if self.transaction_summary_capacity > 5000 {
            return Err(
                "transaction-summary.transaction-summary-capacity should not be larger than 5000"
                    .into(),
            );
        }
        Ok(())
    }
}

/// A flattened config value (a JSON scalar or array; nested maps are
/// flattened away).
pub type FlatValue = serde_json::Value;

/// Go `dynamicConfigItems`.
pub const DYNAMIC_CONFIG_ITEMS: &[&str] = &[
    "Performance.MaxProcs",
    "Performance.MaxMemory",
    "Performance.CrossJoin",
    "Performance.PseudoEstimateRatio",
    "Performance.StmtCountLimit",
    "Performance.TCPKeepAlive",
    "TiKVClient.StoreLimit",
    "Log.Level",
    "Log.ExpensiveThreshold",
    "Instance.SlowThreshold",
    "Instance.CheckMb4ValueInUTF8",
    "TxnLocalLatches.Capacity",
    "CompatibleKillQuery",
    "TreatOldVersionUTF8AsUTF8MB4",
    "OpenTracing.Enable",
];

/// Go `MergeConfigItems`: applies runtime-dynamic fields and reports all
/// other changed fields as rejected.
pub fn merge_config_items(dst: &mut Config, new: &Config) -> (Vec<String>, Vec<String>) {
    let mut accepted = Vec::new();
    macro_rules! merge_dynamic {
        ($name:literal, $dst:expr, $new:expr) => {
            if $dst != $new {
                $dst = $new.clone();
                accepted.push($name.to_owned());
            }
        };
    }

    merge_dynamic!(
        "Performance.MaxProcs",
        dst.performance.max_procs,
        new.performance.max_procs
    );
    merge_dynamic!(
        "Performance.MaxMemory",
        dst.performance.max_memory,
        new.performance.max_memory
    );
    merge_dynamic!(
        "Performance.CrossJoin",
        dst.performance.cross_join,
        new.performance.cross_join
    );
    merge_dynamic!(
        "Performance.PseudoEstimateRatio",
        dst.performance.pseudo_estimate_ratio,
        new.performance.pseudo_estimate_ratio
    );
    merge_dynamic!(
        "Performance.StmtCountLimit",
        dst.performance.stmt_count_limit,
        new.performance.stmt_count_limit
    );
    merge_dynamic!(
        "Performance.TCPKeepAlive",
        dst.performance.tcp_keep_alive,
        new.performance.tcp_keep_alive
    );
    merge_dynamic!(
        "TiKVClient.StoreLimit",
        dst.tikv_client.store_limit,
        new.tikv_client.store_limit
    );
    merge_dynamic!("Log.Level", dst.log.level, new.log.level);
    merge_dynamic!(
        "Log.ExpensiveThreshold",
        dst.log.expensive_threshold,
        new.log.expensive_threshold
    );
    merge_dynamic!(
        "Instance.SlowThreshold",
        dst.instance.slow_threshold,
        new.instance.slow_threshold
    );
    merge_dynamic!(
        "Instance.CheckMb4ValueInUTF8",
        dst.instance.check_mb4_value_in_utf8,
        new.instance.check_mb4_value_in_utf8
    );
    merge_dynamic!(
        "TxnLocalLatches.Capacity",
        dst.txn_local_latches.capacity,
        new.txn_local_latches.capacity
    );
    merge_dynamic!(
        "CompatibleKillQuery",
        dst.compatible_kill_query,
        new.compatible_kill_query
    );
    merge_dynamic!(
        "TreatOldVersionUTF8AsUTF8MB4",
        dst.treat_old_version_utf8_as_utf8mb4,
        new.treat_old_version_utf8_as_utf8mb4
    );
    merge_dynamic!(
        "OpenTracing.Enable",
        dst.open_tracing.enable,
        new.open_tracing.enable
    );

    let dst_value = serde_json::to_value(&*dst).expect("Config must serialize");
    let new_value = serde_json::to_value(new).expect("Config must serialize");
    let mut rejected = Vec::new();
    collect_config_differences(&dst_value, &new_value, "", &mut rejected);
    if dst.txn_local_latches.enabled != new.txn_local_latches.enabled {
        rejected.push("TxnLocalLatches.Enabled".to_owned());
    }
    if dst.performance.mem_profile_interval != new.performance.mem_profile_interval {
        rejected.push("Performance.MemProfileInterval".to_owned());
    }
    rejected.sort();
    rejected.dedup();
    (accepted, rejected)
}

fn collect_config_differences(
    left: &serde_json::Value,
    right: &serde_json::Value,
    prefix: &str,
    differences: &mut Vec<String>,
) {
    match (left, right) {
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            let keys: BTreeSet<_> = left.keys().chain(right.keys()).collect();
            for key in keys {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                match (left.get(key), right.get(key)) {
                    (Some(left), Some(right)) => {
                        collect_config_differences(left, right, &path, differences);
                    }
                    _ => differences.push(path),
                }
            }
        }
        _ if left != right => differences.push(prefix.to_owned()),
        _ => {}
    }
}

/// Flattens a nested config map into dotted keys (Go `FlattenConfigItems`).
/// Arrays are not flattened.
pub fn flatten_config_items(
    nested: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, FlatValue> {
    let mut flat = HashMap::new();
    flatten(&mut flat, &serde_json::Value::Object(nested.clone()), "");
    flat
}

fn flatten(flat: &mut HashMap<String, FlatValue>, nested: &serde_json::Value, prefix: &str) {
    match nested {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten(flat, v, &path);
            }
        }
        // Don't flatten arrays or scalars.
        other => {
            flat.insert(prefix.to_string(), other.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_allowed_packet_validity() {
        assert!(valid_max_allowed_packet(1024));
        assert!(valid_max_allowed_packet(64 << 20));
        assert!(valid_max_allowed_packet(1 << 30));
        assert!(!valid_max_allowed_packet(0));
        assert!(!valid_max_allowed_packet(1023));
        assert!(!valid_max_allowed_packet(1500)); // not a multiple of 1024
        assert!(!valid_max_allowed_packet((1 << 30) + 1024));
    }

    // Go TestMergeConfigItems.
    #[test]
    fn test_merge_config_items() {
        let original = Config::default();
        let mut old = original.clone();
        let mut new = old.clone();

        new.performance.max_procs = 123;
        new.performance.max_memory = 123;
        new.performance.cross_join = false;
        new.performance.pseudo_estimate_ratio = 123.0;
        new.tikv_client.store_limit = 123;

        new.store = crate::store::StoreType("tiflash".to_owned());
        new.port = 2333;
        new.advertise_address = "1.2.3.4".to_owned();
        new.instance.slow_threshold = 2345;

        let (accepted, rejected) = merge_config_items(&mut old, &new);
        assert_eq!(accepted.len(), 6);
        assert_eq!(rejected.len(), 3);
        assert!(accepted
            .iter()
            .all(|item| DYNAMIC_CONFIG_ITEMS.contains(&item.as_str())));
        assert!(rejected
            .iter()
            .all(|item| !DYNAMIC_CONFIG_ITEMS.contains(&item.as_str())));

        assert_eq!(old.performance.max_procs, new.performance.max_procs);
        assert_eq!(old.performance.max_memory, new.performance.max_memory);
        assert_eq!(old.performance.cross_join, new.performance.cross_join);
        assert_eq!(
            old.performance.pseudo_estimate_ratio,
            new.performance.pseudo_estimate_ratio
        );
        assert_eq!(old.tikv_client.store_limit, new.tikv_client.store_limit);
        assert_eq!(old.instance.slow_threshold, new.instance.slow_threshold);

        assert_eq!(old.store, original.store);
        assert_eq!(old.port, original.port);
        assert_eq!(old.advertise_address, original.advertise_address);
    }

    // Covers the standalone parts of Go
    // TestErrorMessageExtensionInvalidRegexp (the Load/deploy-mode paths
    // land with the Config struct).
    #[test]
    fn error_message_extension_prepare() {
        // Invalid regexp, strict.
        let exts = vec![ErrorMessageExtension {
            pattern: "[".into(),
            suffix: "x".into(),
        }];
        assert!(prepare_error_message_extensions(&exts, false)
            .unwrap_err()
            .contains("invalid error-msg-extension regexp"));

        // Whitespace-only pattern, strict.
        let exts = vec![ErrorMessageExtension {
            pattern: " \t".into(),
            suffix: "x".into(),
        }];
        assert!(prepare_error_message_extensions(&exts, false)
            .unwrap_err()
            .contains("empty error-msg-extension pattern"));

        // ignore_invalid collects the first error and drops bad entries.
        let exts = vec![
            ErrorMessageExtension {
                pattern: "".into(),
                suffix: "empty".into(),
            },
            ErrorMessageExtension {
                pattern: "^ok$".into(),
                suffix: "good".into(),
            },
        ];
        let (prepared, first) = prepare_error_message_extensions(&exts, true).unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].pattern, "^ok$");
        assert!(first.unwrap().contains("empty error-msg-extension pattern"));

        // Sort: longer patterns first, then lexicographic.
        let exts = vec![
            ErrorMessageExtension {
                pattern: "ab".into(),
                suffix: "".into(),
            },
            ErrorMessageExtension {
                pattern: "abcd".into(),
                suffix: "".into(),
            },
            ErrorMessageExtension {
                pattern: "aa".into(),
                suffix: "".into(),
            },
        ];
        let (prepared, _) = prepare_error_message_extensions(&exts, false).unwrap();
        assert_eq!(
            prepared
                .iter()
                .map(|e| e.pattern.as_str())
                .collect::<Vec<_>>(),
            vec!["abcd", "aa", "ab"]
        );
    }

    #[test]
    fn cse_and_trx_summary() {
        let cse = Cse::default_config();
        assert!(cse.valid());
        assert!(cse.is_tiflash_enabled());
        assert!(!cse.is_columnar_store_enabled());
        assert!(Cse {
            columnar_store_type: "both".into(),
            columnar_collect_timeout: 0
        }
        .is_columnar_store_enabled());
        assert!(!Cse {
            columnar_store_type: "bogus".into(),
            columnar_collect_timeout: 0
        }
        .valid());

        let mut ts = TrxSummary::default();
        assert_eq!(ts.transaction_summary_capacity, 500);
        ts.valid().unwrap();
        ts.transaction_summary_capacity = 5001;
        assert!(ts.valid().is_err());
    }

    // Go TestFlattenConfig.
    #[test]
    fn flatten_config() {
        let json = r#"{
            "k0": 233333,
            "k1": "v1",
            "k2": ["v2-1", "v2-2", "v2-3"],
            "k3": [{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}],
            "k4": { "k4-1": [1, 2, 3, 4], "k4-2": [5, 6, 7, 8], "k4-3": [666] }
        }"#;
        let nested: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(json).unwrap();
        let flat = flatten_config_items(&nested);
        assert_eq!(flat.len(), 7);
        assert_eq!(flat["k0"], serde_json::json!(233333));
        assert_eq!(flat["k1"], serde_json::json!("v1"));
        assert_eq!(flat["k2"], serde_json::json!(["v2-1", "v2-2", "v2-3"]));
        assert_eq!(
            flat["k3"],
            serde_json::json!([{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}])
        );
        assert_eq!(flat["k4.k4-1"], serde_json::json!([1, 2, 3, 4]));
        assert_eq!(flat["k4.k4-2"], serde_json::json!([5, 6, 7, 8]));
        assert_eq!(flat["k4.k4-3"], serde_json::json!([666]));
    }
}
