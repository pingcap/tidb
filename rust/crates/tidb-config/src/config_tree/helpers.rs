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
//! and `FlattenConfigItems`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::configtypes::Duration;

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
