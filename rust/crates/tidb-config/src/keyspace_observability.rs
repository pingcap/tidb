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

//! Transcreation of Go `pkg/config/keyspace_observability.go`: mapping
//! keyspace metadata into metric labels and slow/statement log fields.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

const METRIC_LABEL_PREFIX: &str = "keyspace_meta_";
const SLOW_LOG_FIELD_PREFIX: &str = "Keyspace_meta_";

/// The `[keyspace-observability]` section (Go `KeyspaceObservability`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct KeyspaceObservability {
    /// Field mappings.
    #[serde(rename = "fields")]
    pub fields: Vec<KeyspaceObservabilityField>,
}

/// One keyspace metadata mapping (Go `KeyspaceObservabilityField`).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct KeyspaceObservabilityField {
    /// Keyspace metadata key.
    #[serde(rename = "source")]
    pub source: String,
    /// Metric label output (must start with `keyspace_meta_`).
    #[serde(rename = "metric-label")]
    pub metric_label: String,
    /// Slow log field output (must start with `Keyspace_meta_`).
    #[serde(rename = "slow-log-field")]
    pub slow_log_field: String,
    /// Statement log field output.
    #[serde(rename = "stmt-log-field")]
    pub stmt_log_field: String,
    /// Whether the metadata entry must exist.
    #[serde(rename = "required")]
    pub required: bool,
}

/// Resolved observability values (Go `KeyspaceObservabilityValues`).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct KeyspaceObservabilityValues {
    /// Metric label -> value.
    pub metric_labels: HashMap<String, String>,
    /// Slow log fields, name-sorted.
    pub slow_log_fields: Vec<KeyspaceObservabilityLogField>,
    /// Statement log field -> value.
    pub stmt_log_fields: HashMap<String, String>,
}

/// One resolved log field (Go `KeyspaceObservabilityLogField`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KeyspaceObservabilityLogField {
    /// Field name.
    pub name: String,
    /// Field value.
    pub value: String,
}

// Prometheus legacy label-name validity: `[a-zA-Z_][a-zA-Z0-9_]*`.
fn valid_prometheus_label_name(label: &str) -> bool {
    let mut bytes = label.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return false,
    }
    bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

impl KeyspaceObservability {
    /// Go `Valid`.
    pub fn valid(&self) -> Result<(), String> {
        let mut metric_labels = HashMap::new();
        let mut slow_log_fields = HashMap::new();
        let mut stmt_log_fields = HashMap::new();
        for (i, field) in self.fields.iter().enumerate() {
            if field.source.is_empty() {
                return Err(format!(
                    "[keyspace-observability.fields.{i}] source cannot be empty"
                ));
            }
            if field.metric_label.is_empty()
                && field.slow_log_field.is_empty()
                && field.stmt_log_field.is_empty()
            {
                return Err(format!(
                    "[keyspace-observability.fields.{i}] at least one output must be set"
                ));
            }
            if !field.metric_label.is_empty() {
                if !valid_prometheus_label_name(&field.metric_label) {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] invalid metric-label {:?}",
                        field.metric_label
                    ));
                }
                let key = field.metric_label.to_lowercase();
                if !key.starts_with(METRIC_LABEL_PREFIX) {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] metric-label {:?} must start with {METRIC_LABEL_PREFIX:?}",
                        field.metric_label
                    ));
                }
                if metric_labels.insert(key, ()).is_some() {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] duplicated metric-label {:?}",
                        field.metric_label
                    ));
                }
            }
            if !field.slow_log_field.is_empty() {
                if !valid_prometheus_label_name(&field.slow_log_field) {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] invalid slow-log-field {:?}",
                        field.slow_log_field
                    ));
                }
                if !field.slow_log_field.starts_with(SLOW_LOG_FIELD_PREFIX) {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] slow-log-field {:?} must start with {SLOW_LOG_FIELD_PREFIX:?}",
                        field.slow_log_field
                    ));
                }
                let key = field.slow_log_field.to_lowercase();
                if slow_log_fields.insert(key, ()).is_some() {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] duplicated slow-log-field {:?}",
                        field.slow_log_field
                    ));
                }
            }
            if !field.stmt_log_field.is_empty() {
                let key = field.stmt_log_field.to_lowercase();
                if stmt_log_fields.insert(key, ()).is_some() {
                    return Err(format!(
                        "[keyspace-observability.fields.{i}] duplicated stmt-log-field {:?}",
                        field.stmt_log_field
                    ));
                }
            }
        }
        Ok(())
    }

    /// Go `Config.ResolveKeyspaceObservability`: resolves against keyspace
    /// metadata.
    pub fn resolve(
        &self,
        values: &HashMap<String, String>,
    ) -> Result<KeyspaceObservabilityValues, String> {
        let mut resolved = KeyspaceObservabilityValues::default();
        for field in &self.fields {
            let Some(value) = values.get(&field.source) else {
                if field.required {
                    return Err(format!(
                        "missing required keyspace metadata entry {:?}",
                        field.source
                    ));
                }
                continue;
            };
            if !field.metric_label.is_empty() {
                resolved
                    .metric_labels
                    .insert(field.metric_label.clone(), value.clone());
            }
            if !field.slow_log_field.is_empty() {
                resolved
                    .slow_log_fields
                    .push(KeyspaceObservabilityLogField {
                        name: field.slow_log_field.clone(),
                        value: value.clone(),
                    });
            }
            if !field.stmt_log_field.is_empty() {
                resolved
                    .stmt_log_fields
                    .insert(field.stmt_log_field.clone(), value.clone());
            }
        }
        resolved.slow_log_fields.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(
        source: &str,
        metric: &str,
        slow: &str,
        stmt: &str,
        required: bool,
    ) -> KeyspaceObservabilityField {
        KeyspaceObservabilityField {
            source: source.into(),
            metric_label: metric.into(),
            slow_log_field: slow.into(),
            stmt_log_field: stmt.into(),
            required,
        }
    }

    #[test]
    fn valid() {
        assert!(KeyspaceObservability::default().valid().is_ok());

        let ok = KeyspaceObservability {
            fields: vec![
                field(
                    "tenant",
                    "keyspace_meta_tenant",
                    "Keyspace_meta_tenant",
                    "tenant",
                    true,
                ),
                field("tier", "", "", "tier", false),
            ],
        };
        ok.valid().unwrap();

        let cases: Vec<(KeyspaceObservability, &str)> = vec![
            (
                KeyspaceObservability {
                    fields: vec![field("", "keyspace_meta_x", "", "", false)],
                },
                "source cannot be empty",
            ),
            (
                KeyspaceObservability {
                    fields: vec![field("s", "", "", "", false)],
                },
                "at least one output must be set",
            ),
            (
                KeyspaceObservability {
                    fields: vec![field("s", "9bad", "", "", false)],
                },
                "invalid metric-label",
            ),
            (
                KeyspaceObservability {
                    fields: vec![field("s", "wrong_prefix", "", "", false)],
                },
                "must start with",
            ),
            (
                KeyspaceObservability {
                    fields: vec![
                        field("a", "keyspace_meta_x", "", "", false),
                        field("b", "Keyspace_meta_X", "", "", false),
                    ],
                },
                "duplicated metric-label",
            ),
            (
                KeyspaceObservability {
                    fields: vec![field("s", "", "keyspace_meta_x", "", false)],
                },
                "must start with",
            ),
            (
                KeyspaceObservability {
                    fields: vec![
                        field("a", "", "Keyspace_meta_x", "", false),
                        field("b", "", "Keyspace_meta_x", "", false),
                    ],
                },
                "duplicated slow-log-field",
            ),
            (
                KeyspaceObservability {
                    fields: vec![
                        field("a", "", "", "x", false),
                        field("b", "", "", "X", false),
                    ],
                },
                "duplicated stmt-log-field",
            ),
        ];
        for (cfg, want) in cases {
            let err = cfg.valid().unwrap_err();
            assert!(err.contains(want), "{err} !~ {want}");
        }
    }

    #[test]
    fn resolve() {
        let cfg = KeyspaceObservability {
            fields: vec![
                field(
                    "tenant",
                    "keyspace_meta_tenant",
                    "Keyspace_meta_tenant",
                    "tenant",
                    true,
                ),
                field("zone", "", "Keyspace_meta_azone", "", false),
                field("missing", "", "", "m", false),
            ],
        };
        let mut values = HashMap::new();
        values.insert("tenant".to_string(), "t1".to_string());
        values.insert("zone".to_string(), "z1".to_string());
        let resolved = cfg.resolve(&values).unwrap();
        assert_eq!(resolved.metric_labels["keyspace_meta_tenant"], "t1");
        // name-sorted slow log fields
        assert_eq!(
            resolved
                .slow_log_fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Keyspace_meta_azone", "Keyspace_meta_tenant"]
        );
        // The "tenant" mapping also outputs a stmt field; the missing
        // optional "m" one is skipped.
        assert_eq!(resolved.stmt_log_fields.len(), 1);
        assert_eq!(resolved.stmt_log_fields["tenant"], "t1");

        let empty = HashMap::new();
        assert!(cfg
            .resolve(&empty)
            .unwrap_err()
            .contains("missing required keyspace metadata entry"));
    }
}
