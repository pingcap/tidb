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

use crate::job::PersistedRawJson;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
};

/// Go `EngineAttribute`: the JSON form of a table's `ENGINE_ATTRIBUTE`.
///
/// Go stores `StorageClass` as a `json.RawMessage`. `None` is the nil byte
/// slice; `Some` retains the exact validated JSON text, including duplicate
/// object members, member order, number lexemes, and insignificant whitespace.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EngineAttribute {
    /// The raw `storage_class` sub-document, if present.
    #[serde(rename = "storage_class", default)]
    pub storage_class: Option<PersistedRawJson>,
}

impl_go_json_merge_object!(EngineAttribute, destination, map, key, {
    if go_json_field_matches(&key, "storage_class") {
        // `json.RawMessage.UnmarshalJSON` copies every valid JSON value,
        // including the exact bytes `null`; only an absent member stays nil.
        destination.storage_class = Some(map.next_value()?);
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(EngineAttribute);

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
#[serde(default)]
pub struct StorageClassDef {
    /// The tier name.
    pub tier: String,
    /// Scope: the partition/table names this applies to.
    #[serde(default)]
    pub names_in: Option<Vec<String>>,
    /// Scope: an upper bound.
    pub less_than: Option<String>,
    /// Scope: an explicit value set.
    #[serde(default)]
    pub values_in: Option<Vec<String>>,
    /// The transition rules.
    #[serde(default)]
    pub transitions: Option<Vec<StorageClassTransitRule>>,
}

impl StorageClassDef {
    /// Go `HasNoScopeDef`: whether no scope (names/less-than/values) is set.
    #[must_use]
    pub fn has_no_scope_def(&self) -> bool {
        self.names_in.as_ref().is_none_or(Vec::is_empty)
            && self.less_than.is_none()
            && self.values_in.as_ref().is_none_or(Vec::is_empty)
    }
}

/// Go `StorageClassSettings`: a set of storage-class definitions.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageClassSettings {
    /// The definitions.
    #[serde(default)]
    pub defs: Option<Vec<Option<StorageClassDef>>>,
}

/// Go `StorageClassTransitRule`: when a tier transition happens.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
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
        // Go's unsigned arithmetic wraps modulo 2^N. Use explicit wrapping
        // operations so debug and release builds have the same observable
        // boundary behavior.
        self.after_days
            .wrapping_mul(86_400)
            .wrapping_add(self.after_seconds)
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
    String::from_utf8(
        crate::serde_helpers::to_go_json(&StorageClassInfo { tier, transitions })
            .unwrap_or_default(),
    )
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_and_valid() {
        // Empty -> default (no storage class).
        let attr = parse_engine_attribute_from_string("").unwrap();
        assert!(attr.storage_class.is_none());
        assert_eq!(
            serde_json::to_string(&attr).unwrap(),
            r#"{"storage_class":null}"#
        );
        assert!(parse_engine_attribute_from_string("null")
            .unwrap()
            .storage_class
            .is_none());

        // Valid JSON with a storage_class sub-document.
        let attr = parse_engine_attribute_from_string(r#"{"storage_class":{"defs":[]}}"#).unwrap();
        assert!(attr.storage_class.is_some());

        // Invalid JSON is an error.
        assert!(parse_engine_attribute_from_string("not json").is_err());
    }

    #[test]
    fn storage_class_retains_raw_message_text_until_go_marshal() {
        let attr = parse_engine_attribute_from_string(
            r#" {"storage_class": { "n":1.00, "dup":1, "dup":2, "text":"<x>" }} "#,
        )
        .unwrap();
        let raw = attr.storage_class.as_ref().unwrap();
        assert_eq!(raw.get(), r#"{ "n":1.00, "dup":1, "dup":2, "text":"<x>" }"#);

        // Go's parent encoder compacts RawMessage whitespace and applies its
        // HTML-safe string escaping while retaining member order, duplicates,
        // and the numeric lexical form.
        assert_eq!(
            String::from_utf8(crate::serde_helpers::to_go_json(&attr).unwrap()).unwrap(),
            r#"{"storage_class":{"n":1.00,"dup":1,"dup":2,"text":"\u003cx\u003e"}}"#
        );

        let explicit_null = parse_engine_attribute_from_string(r#"{"storage_class":null}"#)
            .unwrap()
            .storage_class
            .unwrap();
        assert_eq!(explicit_null.get(), "null");
        assert!(parse_engine_attribute_from_string(" \n null\t ")
            .unwrap()
            .storage_class
            .is_none());
    }

    #[test]
    fn engine_attribute_uses_go_object_member_matching_and_overwrite_order() {
        let missing = parse_engine_attribute_from_string(r#"{"unknown":{"x":1}}"#).unwrap();
        assert!(missing.storage_class.is_none());

        let explicit_null =
            parse_engine_attribute_from_string(r#"{"storage_class":null}"#).unwrap();
        assert_eq!(explicit_null.storage_class.unwrap().get(), "null");

        let duplicate = parse_engine_attribute_from_string(
            r#"{"storage_class":{"first":1},"storage_class":{"last":2}}"#,
        )
        .unwrap();
        assert_eq!(duplicate.storage_class.unwrap().get(), r#"{"last":2}"#);

        // `encoding/json` falls back to Unicode SimpleFold after checking for
        // an exact tag. Both the ordinary case fold and long-s equivalence
        // reach the same unique field, and later members still win.
        let folded = parse_engine_attribute_from_string(
            r#"{"STORAGE_CLASS":1,"\u017ftorage_cla\u017fs":2}"#,
        )
        .unwrap();
        assert_eq!(folded.storage_class.unwrap().get(), "2");

        // A key with a different punctuation pattern is unknown, not folded.
        let punctuation = parse_engine_attribute_from_string(r#"{"storage-class":1}"#).unwrap();
        assert!(punctuation.storage_class.is_none());

        // ParseEngineAttributeFromString returns no partially decoded value on
        // either a top-level type mismatch or a syntax error, exactly like its
        // Go `return nil, err` path.
        assert!(parse_engine_attribute_from_string(r#"[1,2]"#).is_err());
        assert!(parse_engine_attribute_from_string(r#"{"storage_class":1,"later":}"#).is_err());
    }

    #[test]
    fn has_no_scope_def() {
        let mut d = StorageClassDef::default();
        assert!(d.has_no_scope_def());
        d.names_in = Some(vec!["p0".to_owned()]);
        assert!(!d.has_no_scope_def());

        let d = StorageClassDef {
            less_than: Some("100".to_owned()),
            ..Default::default()
        };
        assert!(!d.has_no_scope_def());

        let zero = serde_json::to_value(StorageClassDef::default()).unwrap();
        assert_eq!(zero["names_in"], serde_json::Value::Null);
        assert_eq!(zero["values_in"], serde_json::Value::Null);
        assert_eq!(zero["transitions"], serde_json::Value::Null);
        let allocated: StorageClassDef = serde_json::from_value(serde_json::json!({
            "names_in": [],
            "values_in": [],
            "transitions": []
        }))
        .unwrap();
        assert_eq!(allocated.names_in, Some(Vec::new()));
        assert_eq!(allocated.values_in, Some(Vec::new()));
        assert!(allocated.transitions.as_ref().is_some_and(Vec::is_empty));

        let settings = serde_json::to_value(StorageClassSettings::default()).unwrap();
        assert_eq!(settings["defs"], serde_json::Value::Null);
        let settings: StorageClassSettings =
            serde_json::from_value(serde_json::json!({"defs": []})).unwrap();
        assert!(settings.defs.as_ref().is_some_and(Vec::is_empty));

        // Go fills omitted scalar fields with their zero values and preserves
        // nil entries in []*StorageClassDef.
        let settings: StorageClassSettings =
            serde_json::from_value(serde_json::json!({"defs": [null, {}]})).unwrap();
        let defs = settings.defs.unwrap();
        assert!(defs[0].is_none());
        assert_eq!(defs[1].as_ref().unwrap().tier, "");

        let transition: StorageClassTransitRule =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(transition.tier, "");
        assert_eq!(transition.after_days, 0);
    }

    #[test]
    fn total_seconds() {
        let r = StorageClassTransitRule {
            tier: "IA".to_owned(),
            after_days: 2,
            after_seconds: 30,
        };
        assert_eq!(r.total_seconds(), 2 * 86400 + 30);

        // Boundary mutation: ordinary `*`/`+` would panic in a debug build,
        // while Go's uint arithmetic wraps.
        let overflow = StorageClassTransitRule {
            after_days: u64::MAX,
            after_seconds: u64::MAX,
            ..Default::default()
        };
        assert_eq!(
            overflow.total_seconds(),
            u64::MAX.wrapping_mul(86_400).wrapping_add(u64::MAX)
        );
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

        assert_eq!(
            build_storage_class_string("<&>", &[StorageClassTransitRule::default()]),
            r#"{"tier":"\u003c\u0026\u003e","transitions":[{"tier":"","after_days":0}]}"#
        );
    }
}
