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

//! `pkg/meta/model/engine_attribute.go`: the JSON `ENGINE_ATTRIBUTE` table
//! property and its storage-class definitions.

use serde::{Deserialize, Serialize};

/// Go `EngineAttribute`: the JSON form of a table's `ENGINE_ATTRIBUTE`.
///
/// Go stores `StorageClass` as a `json.RawMessage`; here it is a
/// [`serde_json::Value`] (absent -> `None`), which preserves the parsed
/// storage-class document that downstream code re-decodes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EngineAttribute {
    /// The raw `storage_class` sub-document, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<serde_json::Value>,
}

/// Go `ParseEngineAttributeFromString`: parses an `EngineAttribute` from a
/// JSON string. An empty string yields the default (no storage class);
/// invalid JSON is an error.
pub fn parse_engine_attribute_from_string(
    input: &str,
) -> Result<EngineAttribute, serde_json::Error> {
    if input.is_empty() {
        return Ok(EngineAttribute::default());
    }
    serde_json::from_str(input)
}

/// The `STANDARD` storage-class tier name.
pub const STORAGE_CLASS_TIER_STANDARD: &str = "STANDARD";
/// The `IA` (infrequent-access) storage-class tier name.
pub const STORAGE_CLASS_TIER_IA: &str = "IA";
/// The default storage-class tier (Go `StorageClassTierDefault`).
pub const STORAGE_CLASS_TIER_DEFAULT: &str = STORAGE_CLASS_TIER_STANDARD;

/// Go `StorageClassDef`: the tier and scope definition of a storage class.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageClassDef {
    /// The tier name.
    pub tier: String,
    /// Scope: the partition/table names this applies to.
    #[serde(default)]
    pub names_in: Vec<String>,
    /// Scope: an upper bound.
    pub less_than: Option<String>,
    /// Scope: an explicit value set.
    #[serde(default)]
    pub values_in: Vec<String>,
    /// The transition rules.
    #[serde(default)]
    pub transitions: Vec<StorageClassTransitRule>,
}

impl StorageClassDef {
    /// Go `HasNoScopeDef`: whether no scope (names/less-than/values) is set.
    #[must_use]
    pub fn has_no_scope_def(&self) -> bool {
        self.names_in.is_empty() && self.less_than.is_none() && self.values_in.is_empty()
    }
}

/// Go `StorageClassSettings`: a set of storage-class definitions.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageClassSettings {
    /// The definitions.
    #[serde(default)]
    pub defs: Vec<StorageClassDef>,
}

/// Go `StorageClassTransitRule`: when a tier transition happens.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageClassTransitRule {
    /// The tier to transition to.
    pub tier: String,
    /// Days after which the transition happens.
    pub after_days: u64,
    /// Extra seconds after which the transition happens.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub after_seconds: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

impl StorageClassTransitRule {
    /// Go `TotalSeconds`: total seconds until the transition.
    #[must_use]
    pub fn total_seconds(&self) -> u64 {
        self.after_days * 86400 + self.after_seconds
    }
}

/// Go `buildStorageClassString`: the JSON string describing a tier and its
/// transitions (just the tier name when there are none).
///
/// Package-private in Go; used by `PartitionDefinition::storage_class_string`.
pub(crate) fn build_storage_class_string(
    tier: &str,
    transitions: &[StorageClassTransitRule],
) -> String {
    if transitions.is_empty() {
        return tier.to_owned();
    }
    #[derive(Serialize)]
    struct StorageClassInfo<'a> {
        tier: &'a str,
        #[serde(skip_serializing_if = "<[_]>::is_empty")]
        transitions: &'a [StorageClassTransitRule],
    }
    // Go ignores the marshal error (it cannot fail for these types).
    serde_json::to_string(&StorageClassInfo { tier, transitions }).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_and_valid() {
        // Empty -> default (no storage class).
        let attr = parse_engine_attribute_from_string("").unwrap();
        assert!(attr.storage_class.is_none());

        // Valid JSON with a storage_class sub-document.
        let attr = parse_engine_attribute_from_string(r#"{"storage_class":{"defs":[]}}"#).unwrap();
        assert!(attr.storage_class.is_some());

        // Invalid JSON is an error.
        assert!(parse_engine_attribute_from_string("not json").is_err());
    }

    #[test]
    fn has_no_scope_def() {
        let mut d = StorageClassDef::default();
        assert!(d.has_no_scope_def());
        d.names_in.push("p0".to_owned());
        assert!(!d.has_no_scope_def());

        let d = StorageClassDef {
            less_than: Some("100".to_owned()),
            ..Default::default()
        };
        assert!(!d.has_no_scope_def());
    }

    #[test]
    fn total_seconds() {
        let r = StorageClassTransitRule {
            tier: "IA".to_owned(),
            after_days: 2,
            after_seconds: 30,
        };
        assert_eq!(r.total_seconds(), 2 * 86400 + 30);
    }

    #[test]
    fn build_storage_class_string_cases() {
        // No transitions -> just the tier name.
        assert_eq!(build_storage_class_string("STANDARD", &[]), "STANDARD");
        // With transitions -> a JSON object carrying them.
        let s = build_storage_class_string(
            "STANDARD",
            &[StorageClassTransitRule {
                tier: "IA".to_owned(),
                after_days: 30,
                after_seconds: 0,
            }],
        );
        assert!(s.contains("\"tier\":\"STANDARD\""));
        assert!(s.contains("\"transitions\""));
        assert!(s.contains("\"tier\":\"IA\""));
        // after_seconds == 0 is omitted (omitempty).
        assert!(!s.contains("after_seconds"));
    }
}
