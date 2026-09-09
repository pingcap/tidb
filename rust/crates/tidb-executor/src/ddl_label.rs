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

//! Complete transcreation of Go `pkg/ddl/label` (`attributes.go`, `rule.go`,
//! `errors.go`): `ATTRIBUTES=...` parsing into PD region labels, and the
//! label rules TiDB builds over table/partition key ranges.
//!
//! One boundary adaptation is named:
//!
//! * Go reads keyspace facts from client-go's `tikv.Codec`. The
//!   [`LabelCodec`] trait carries the three facts the source consumes —
//!   whether keyspace meta exists, the keyspace ID, and region-range
//!   encoding — with [`CodecV1`] as the classic, keyspace-less
//!   implementation.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use tidb_codec::table_key::gen_table_prefix;
use tidb_codec::{encode_bytes, encoded_bytes_len};
use tidb_datatype::go_runtime::{GoSharedSlice, GoSliceElementLayout};
use tidb_model::go_runtime::GoShared;

/// Go's private `keyspaceKey`.
const KEYSPACE_KEY: &str = "keyspace";
/// Go's private `dbKey`.
const DB_KEY: &str = "db";
/// Go's private `tableKey`.
const TABLE_KEY: &str = "table";
/// Go's private `partitionKey`.
const PARTITION_KEY: &str = "partition";

/// Go `IDPrefix`.
pub const ID_PREFIX: &str = "schema";
/// Go `KeyspacePrefix`.
pub const KEYSPACE_PREFIX: &str = "keyspace";
/// Go's private `ruleType`.
pub const RULE_TYPE: &str = "key-range";

/// Go `RuleIndexDefault`.
pub const RULE_INDEX_DEFAULT: i64 = 0;
/// Go `RuleIndexDatabase`.
pub const RULE_INDEX_DATABASE: i64 = 1;
/// Go `RuleIndexTable`.
pub const RULE_INDEX_TABLE: i64 = 2;
/// Go `RuleIndexPartition`.
pub const RULE_INDEX_PARTITION: i64 = 3;

/// Go `pd.RegionLabel`, including the optional expiry fields preserved by the
/// package's `pd.LabelRule` alias boundary.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default)]
pub struct RegionLabel {
    /// The label key.
    pub key: String,
    /// The label value.
    pub value: String,
    /// PD's optional expiry duration.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ttl: String,
    /// PD's optional expiry start time.
    #[serde(default, rename = "start_at", skip_serializing_if = "String::is_empty")]
    pub start_at: String,
}

/// The failures `pkg/ddl/label` reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LabelError {
    /// Go `ErrInvalidAttributesFormat`, wrapped as `%w: %s` around the
    /// offending attribute.
    InvalidAttributesFormat(String),
    /// Go `Add`'s conflict error between two restored labels.
    Conflict(String, String),
    /// The attribute list itself failed to parse (Go's YAML error surface).
    MalformedList(String),
}

impl std::fmt::Display for LabelError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAttributesFormat(attr) => {
                write!(
                    formatter,
                    "attributes should be in format 'key=value': {attr}"
                )
            }
            Self::Conflict(new, existing) => {
                write!(formatter, "'{new}' and '{existing}' are conflicted")
            }
            Self::MalformedList(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for LabelError {}

/// Go `AttributesCompatibility`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttributesCompatibility {
    /// Go `AttributesCompatible`.
    Compatible,
    /// Go `AttributesIncompatible`.
    Incompatible,
    /// Go `AttributesDuplicated`.
    Duplicated,
}

/// Go `NewLabel`: `key=value` with both sides trimmed and non-empty.
pub fn new_label(attr: &str) -> Result<RegionLabel, LabelError> {
    let mut parts = attr.split('=');
    let (Some(key), Some(value), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(LabelError::InvalidAttributesFormat(attr.to_owned()));
    };
    let key = key.trim();
    if key.is_empty() {
        return Err(LabelError::InvalidAttributesFormat(attr.to_owned()));
    }
    let value = value.trim();
    if value.is_empty() {
        return Err(LabelError::InvalidAttributesFormat(attr.to_owned()));
    }
    Ok(RegionLabel {
        key: key.to_owned(),
        value: value.to_owned(),
        ..RegionLabel::default()
    })
}

/// Go `RestoreRegionLabel`.
#[must_use]
pub fn restore_region_label(label: &RegionLabel) -> String {
    format!("{}={}", label.key, label.value)
}

/// Go `CompatibleWith`.
#[must_use]
pub fn compatible_with(label: &RegionLabel, other: &RegionLabel) -> AttributesCompatibility {
    if label.key != other.key {
        return AttributesCompatibility::Compatible;
    }
    if label.value == other.value {
        return AttributesCompatibility::Duplicated;
    }
    AttributesCompatibility::Incompatible
}

/// Go `NewLabels`: parses each attribute and folds it in through [`add`], so
/// duplicates collapse and conflicts fail.
pub fn new_labels(attrs: &[String]) -> Result<Vec<RegionLabel>, LabelError> {
    let mut labels = Vec::with_capacity(attrs.len());
    for attr in attrs {
        let label = new_label(attr)?;
        add(&mut labels, label)?;
    }
    Ok(labels)
}

/// Go `RestoreRegionLabels`: quotes each label, skipping the reserved
/// db/table/partition keys — and the keyspace key on next-generation kernels.
#[must_use]
pub fn restore_region_labels(labels: &[RegionLabel]) -> String {
    let mut restored = String::new();
    let mut written = 0;
    for label in labels {
        match label.key.as_str() {
            DB_KEY | TABLE_KEY | PARTITION_KEY => continue,
            KEYSPACE_KEY if tidb_config::kerneltype::is_next_gen() => continue,
            _ => {}
        }
        if written > 0 {
            restored.push(',');
        }
        restored.push('"');
        restored.push_str(&restore_region_label(label));
        restored.push('"');
        written += 1;
    }
    restored
}

/// Go `Add`: appends unless the label duplicates (silently kept once) or
/// conflicts with an existing key.
pub fn add(labels: &mut Vec<RegionLabel>, label: RegionLabel) -> Result<(), LabelError> {
    for existing in labels.iter() {
        match compatible_with(&label, existing) {
            AttributesCompatibility::Compatible => continue,
            AttributesCompatibility::Duplicated => return Ok(()),
            AttributesCompatibility::Incompatible => {
                return Err(LabelError::Conflict(
                    restore_region_label(&label),
                    restore_region_label(existing),
                ));
            }
        }
    }
    labels.push(label);
    Ok(())
}

/// The comma-separated, optionally quoted attribute list Go feeds through
/// YAML. An empty element (`a,,b` or a trailing comma) is malformed, as
/// YAML's flow grammar makes it.
fn parse_attributes_list(attributes: &str) -> Result<Vec<String>, LabelError> {
    serde_yaml::from_str::<Vec<String>>(&format!("[{attributes}]"))
        .map_err(|error| LabelError::MalformedList(error.to_string()))
}

/// The keyspace facts Go reads from client-go's `tikv.Codec`.
pub trait LabelCodec {
    /// Go `codec.GetKeyspaceMeta() != nil`.
    fn has_keyspace_meta(&self) -> bool;
    /// Go `codec.GetKeyspaceID()`.
    fn keyspace_id(&self) -> u32;
    /// Go `codec.EncodeRegionRange(start, end)`.
    fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>);
}

/// Go `tikv.NewCodecV1`: the classic, keyspace-less codec. Its region-range
/// encoding is never consulted by this package (the classic arm encodes with
/// `codec.EncodeBytes` directly).
#[derive(Clone, Copy, Debug, Default)]
pub struct CodecV1;

impl LabelCodec for CodecV1 {
    fn has_keyspace_meta(&self) -> bool {
        false
    }

    fn keyspace_id(&self) -> u32 {
        0
    }

    fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (mem_comparable(start), mem_comparable(end))
    }
}

impl LabelCodec for tikv_client::request::ApiV2Codec {
    fn has_keyspace_meta(&self) -> bool {
        true
    }

    fn keyspace_id(&self) -> u32 {
        self.keyspace_id()
    }

    fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        self.encode_region_range(start, end)
    }
}

fn mem_comparable(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(encoded_bytes_len(key.len()));
    encode_bytes(&mut encoded, key);
    encoded
}

/// Go `UseKeyspaceAwareRules`.
#[must_use]
pub fn use_keyspace_aware_rules(codec: &dyn LabelCodec) -> bool {
    tidb_config::kerneltype::is_next_gen() && codec.has_keyspace_meta()
}

/// Go `NewRuleID`.
#[must_use]
pub fn new_rule_id(
    codec: &dyn LabelCodec,
    db_name: &str,
    table_name: &str,
    part_name: &str,
) -> String {
    let mut id = if part_name.is_empty() {
        format!("{ID_PREFIX}/{db_name}/{table_name}")
    } else {
        format!("{ID_PREFIX}/{db_name}/{table_name}/{part_name}")
    };
    if use_keyspace_aware_rules(codec) {
        id = format!("{KEYSPACE_PREFIX}/{}/{id}", codec.keyspace_id());
    }
    id
}

/// Go `RestoreRuleID`: strips the keyspace prefix on next-generation kernels.
#[must_use]
pub fn restore_rule_id(rule_id: &str) -> String {
    if !tidb_config::kerneltype::is_next_gen() {
        return rule_id.to_owned();
    }
    let parts: Vec<&str> = rule_id.split('/').collect();
    if parts.len() >= 3 && parts[0] == KEYSPACE_PREFIX && parts[2] == ID_PREFIX {
        return parts[2..].join("/");
    }
    rule_id.to_owned()
}

/// Go `label.Rule` (`pd.LabelRule`): labels bound to a key range.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Rule {
    /// Go `ID`.
    pub id: String,
    /// Go `Index`.
    pub index: i64,
    /// Go `Labels`.
    pub labels: GoSharedSlice<RegionLabel>,
    /// Go `RuleType`.
    #[serde(rename = "rule_type")]
    pub rule_type: String,
    /// Go `Data`: arbitrary PD JSON; [`Self::reset`] writes one key-range map
    /// per table ID.
    pub data: Option<GoShared<serde_json::Value>>,
}

impl Rule {
    /// Go `NewRule`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `ApplyAttributesSpec`. `None` attributes is the `DEFAULT` spec,
    /// which clears the labels.
    pub fn apply_attributes_spec(
        &mut self,
        spec: &tidb_ast::AttributesSpec,
    ) -> Result<(), LabelError> {
        let Some(attributes) = &spec.attributes else {
            self.labels = GoSharedSlice::from_vec(Vec::new());
            return Ok(());
        };
        let items = parse_attributes_list(attributes)?;
        self.labels = GoSharedSlice::from_vec_with_capacity(new_labels(&items)?, items.len());
        Ok(())
    }

    /// Go `Clone`.
    #[must_use]
    pub fn clone_rule(&self) -> Self {
        self.clone()
    }

    /// Go `Reset`: rewrites the rule for a table or partition. The reserved
    /// db/table/partition (and, under keyspace-aware rules, keyspace) labels
    /// are updated in place or appended, IDs are sorted, and each ID becomes
    /// one hex key range over its table prefix.
    ///
    /// Faithful to the source, a rule whose labels are empty gets only its ID:
    /// no rule type, no data, no index.
    pub fn reset(
        &mut self,
        codec: &dyn LabelCodec,
        db_name: &str,
        table_name: &str,
        part_name: &str,
        ids: &[i64],
    ) -> &mut Self {
        let is_partition = !part_name.is_empty();
        let use_keyspace = use_keyspace_aware_rules(codec);
        self.id = new_rule_id(codec, db_name, table_name, part_name);
        if self.labels.is_empty() {
            return self;
        }
        let labels = &mut self.labels;

        let mut has_keyspace = false;
        let mut has_db = false;
        let mut has_table = false;
        let mut has_partition = false;
        for index in 0..labels.len() {
            labels.update(index, |label| match label.key.as_str() {
                KEYSPACE_KEY if use_keyspace => {
                    label.value = codec.keyspace_id().to_string();
                    has_keyspace = true;
                }
                DB_KEY => {
                    label.value = db_name.to_owned();
                    has_db = true;
                }
                TABLE_KEY => {
                    label.value = table_name.to_owned();
                    has_table = true;
                }
                PARTITION_KEY if is_partition => {
                    label.value = part_name.to_owned();
                    has_partition = true;
                }
                _ => {}
            });
        }
        if use_keyspace && !has_keyspace {
            labels.push_go(
                RegionLabel {
                    key: KEYSPACE_KEY.to_owned(),
                    value: codec.keyspace_id().to_string(),
                    ..RegionLabel::default()
                },
                64,
                GoSliceElementLayout::PointerBearing,
            );
        }
        if !has_db {
            labels.push_go(
                RegionLabel {
                    key: DB_KEY.to_owned(),
                    value: db_name.to_owned(),
                    ..RegionLabel::default()
                },
                64,
                GoSliceElementLayout::PointerBearing,
            );
        }
        if !has_table {
            labels.push_go(
                RegionLabel {
                    key: TABLE_KEY.to_owned(),
                    value: table_name.to_owned(),
                    ..RegionLabel::default()
                },
                64,
                GoSliceElementLayout::PointerBearing,
            );
        }
        if is_partition && !has_partition {
            labels.push_go(
                RegionLabel {
                    key: PARTITION_KEY.to_owned(),
                    value: part_name.to_owned(),
                    ..RegionLabel::default()
                },
                64,
                GoSliceElementLayout::PointerBearing,
            );
        }

        self.rule_type = RULE_TYPE.to_owned();
        let mut sorted_ids = ids.to_vec();
        sorted_ids.sort_unstable();
        self.data = Some(GoShared::new(
            sorted_ids
                .iter()
                .map(|&id| {
                    let (start_key, end_key) = if use_keyspace {
                        // Label rules are consumed as region boundary keys, so V2
                        // encodes the whole outer key rather than prefixing a
                        // mem-encoded table key.
                        codec.encode_region_range(
                            &gen_table_prefix(id),
                            &gen_table_prefix(id.wrapping_add(1)),
                        )
                    } else {
                        (
                            mem_comparable(&gen_table_prefix(id)),
                            mem_comparable(&gen_table_prefix(id.wrapping_add(1))),
                        )
                    };
                    let mut range = BTreeMap::new();
                    range.insert("start_key".to_owned(), hex_lower(&start_key));
                    range.insert("end_key".to_owned(), hex_lower(&end_key));
                    range
                })
                .map(|range| serde_json::to_value(range).expect("string key-range maps serialize"))
                .collect::<Vec<_>>()
                .into(),
        ));
        self.index = if is_partition {
            RULE_INDEX_PARTITION
        } else {
            RULE_INDEX_TABLE
        };
        self
    }
}

impl PartialEq for Rule {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.index == other.index
            && self.labels.is_allocated() == other.labels.is_allocated()
            && self.labels.snapshot() == other.labels.snapshot()
            && self.rule_type == other.rule_type
            && match (&self.data, &other.data) {
                (Some(left), Some(right)) => left.deep_value_eq(right),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for Rule {}

impl std::fmt::Display for Rule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&serde_json::to_string(self).unwrap_or_default())
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Go `pd.LabelRulePatch`: rules to set and rule IDs to delete.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct LabelRulePatch {
    /// Go `SetRules`.
    #[serde(rename = "sets")]
    pub set_rules: GoSharedSlice<Rule>,
    /// Go `DeleteRules`.
    #[serde(rename = "deletes")]
    pub delete_rules: GoSharedSlice<String>,
}

impl PartialEq for LabelRulePatch {
    fn eq(&self, other: &Self) -> bool {
        self.set_rules.is_allocated() == other.set_rules.is_allocated()
            && self.set_rules.snapshot() == other.set_rules.snapshot()
            && self.delete_rules.is_allocated() == other.delete_rules.is_allocated()
            && self.delete_rules.snapshot() == other.delete_rules.snapshot()
    }
}

impl Eq for LabelRulePatch {}

/// Go `NewRulePatch`.
#[must_use]
pub fn new_rule_patch(set_rules: Vec<Rule>, delete_rules: Vec<String>) -> LabelRulePatch {
    LabelRulePatch {
        set_rules: set_rules.into(),
        delete_rules: delete_rules.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(attributes: &str) -> tidb_ast::AttributesSpec {
        tidb_ast::AttributesSpec {
            attributes: Some(attributes.to_owned()),
        }
    }

    fn default_spec() -> tidb_ast::AttributesSpec {
        tidb_ast::AttributesSpec { attributes: None }
    }

    fn labels(rule: &Rule) -> Vec<RegionLabel> {
        rule.labels.snapshot()
    }

    fn ranges(rule: &Rule) -> Vec<serde_json::Value> {
        rule.data
            .as_ref()
            .expect("Reset writes Data")
            .read()
            .as_array()
            .expect("Reset writes an array")
            .clone()
    }

    // Go `TestNewLabel`.
    #[test]
    fn labels_parse_and_trim() {
        for input in ["merge_option=allow", " merge_option=allow "] {
            let label = new_label(input).unwrap();
            assert_eq!(label.key, "merge_option");
            assert_eq!(label.value, "allow");
        }
        for bad in ["merge_option", "=allow", "merge_option=", "a=b=c", " = "] {
            assert!(new_label(bad).is_err(), "{bad}");
        }
    }

    // Go `TestRestoreLabel`.
    #[test]
    fn labels_restore_to_key_equals_value() {
        let label = new_label(" merge_option=allow  ").unwrap();
        assert_eq!(restore_region_label(&label), "merge_option=allow");
    }

    // Go `TestNewLabels`: duplicates collapse.
    #[test]
    fn label_lists_deduplicate() {
        assert!(new_labels(&[]).unwrap().is_empty());

        let labels =
            new_labels(&["merge_option=allow".to_owned(), "key=value".to_owned()]).unwrap();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key, "merge_option");
        assert_eq!(labels[1].key, "key");

        let labels = new_labels(&[
            "merge_option=allow".to_owned(),
            "merge_option=allow".to_owned(),
        ])
        .unwrap();
        assert_eq!(labels.len(), 1);
    }

    // Go `TestAddLabels`: compatible appends, duplicate skips, conflict errs.
    #[test]
    fn adding_labels_checks_compatibility() {
        let mut labels = new_labels(&["merge_option=allow".to_owned()]).unwrap();
        add(&mut labels, new_label("somethingelse=true").unwrap()).unwrap();
        assert_eq!(labels.len(), 2);

        let mut labels = new_labels(&["key=value".to_owned()]).unwrap();
        add(&mut labels, new_label("key=value").unwrap()).unwrap();
        assert_eq!(labels.len(), 1);

        let mut labels = new_labels(&["key=value1".to_owned()]).unwrap();
        let error = add(&mut labels, new_label("key=value").unwrap()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "'key=value' and 'key=value1' are conflicted"
        );
    }

    // Go `TestRestoreLabels`, classic-kernel arm: reserved keys are hidden,
    // the keyspace key is not.
    #[test]
    fn restoring_labels_hides_reserved_keys() {
        let merge = new_label("merge_option=allow").unwrap();
        let plain = new_label("key=value").unwrap();
        let db = new_label("db=d1").unwrap();
        let table = new_label("table=t1").unwrap();
        let partition = new_label("partition=p1").unwrap();
        let keyspace = new_label("keyspace=42").unwrap();

        assert_eq!(restore_region_labels(&[]), "");
        assert_eq!(
            restore_region_labels(&[merge.clone(), plain.clone()]),
            r#""merge_option=allow","key=value""#
        );
        assert_eq!(restore_region_labels(&[db.clone(), table, partition]), "");
        assert_eq!(
            restore_region_labels(&[merge.clone(), plain, db]),
            r#""merge_option=allow","key=value""#
        );
        if tidb_config::kerneltype::is_next_gen() {
            assert_eq!(
                restore_region_labels(&[merge.clone(), keyspace.clone()]),
                r#""merge_option=allow""#
            );
            assert_eq!(
                restore_region_labels(&[keyspace, merge]),
                r#""merge_option=allow""#
            );
        } else {
            assert_eq!(
                restore_region_labels(&[merge.clone(), keyspace.clone()]),
                r#""merge_option=allow","keyspace=42""#
            );
            assert_eq!(
                restore_region_labels(&[keyspace, merge]),
                r#""keyspace=42","merge_option=allow""#
            );
        }
    }

    // Go `TestApplyAttributesSpec`.
    #[test]
    fn attribute_specs_parse_or_reject() {
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&spec("key=value,key1=value1"))
            .unwrap();
        assert_eq!(labels(&rule).len(), 2);
        assert_eq!(labels(&rule)[0].key, "key");
        assert_eq!(labels(&rule)[0].value, "value");
        assert_eq!(labels(&rule)[1].key, "key1");
        assert_eq!(labels(&rule)[1].value, "value1");

        for bad in [
            "key=value,,key1=value1",
            "key-value,key1=value1",
            "key=,key1=value1",
            "=value,key1=value1",
        ] {
            assert!(rule.apply_attributes_spec(&spec(bad)).is_err(), "{bad}");
        }

        // The quoted forms the SHOW output round-trips through.
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&spec(r#""merge_option=allow","key=value""#))
            .unwrap();
        assert_eq!(labels(&rule).len(), 2);
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&spec(r#""key=a,b",other=value"#))
            .unwrap();
        assert_eq!(labels(&rule)[0], new_label("key=a,b").unwrap());
    }

    // Go `TestDefaultOrEmpty`.
    #[test]
    fn default_and_empty_specs_leave_no_labels() {
        for empty in [spec(""), default_spec()] {
            let mut rule = Rule::new();
            rule.apply_attributes_spec(&empty).unwrap();
            rule.reset(&CodecV1, "db", "t", "", &[1]);
            assert!(labels(&rule).is_empty());
        }
    }

    // Go `TestReset`, including the exact hex key ranges.
    #[test]
    fn reset_builds_ids_labels_and_key_ranges() {
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&spec("key=value")).unwrap();

        rule.reset(&CodecV1, "db1", "t1", "", &[1, 2, 3]);
        assert_eq!(rule.id, "schema/db1/t1");
        assert_eq!(rule.rule_type, RULE_TYPE);
        assert_eq!(labels(&rule).len(), 3);
        assert_eq!(labels(&rule)[0].value, "value");
        assert_eq!(labels(&rule)[1].value, "db1");
        assert_eq!(labels(&rule)[2].value, "t1");
        assert_eq!(rule.index, RULE_INDEX_TABLE);

        let expected = [
            (
                "7480000000000000ff0100000000000000f8",
                "7480000000000000ff0200000000000000f8",
            ),
            (
                "7480000000000000ff0200000000000000f8",
                "7480000000000000ff0300000000000000f8",
            ),
            (
                "7480000000000000ff0300000000000000f8",
                "7480000000000000ff0400000000000000f8",
            ),
        ];
        for (range, (start, end)) in ranges(&rule).iter().zip(expected) {
            assert_eq!(range["start_key"], start);
            assert_eq!(range["end_key"], end);
        }

        let cloned = rule.clone_rule();
        assert_eq!(cloned, rule);

        rule.reset(&CodecV1, "db2", "t2", "p2", &[2]);
        assert_eq!(rule.id, "schema/db2/t2/p2");
        assert_eq!(labels(&rule).len(), 4);
        assert_eq!(labels(&rule)[0].value, "value");
        assert_eq!(labels(&rule)[1].value, "db2");
        assert_eq!(labels(&rule)[2].value, "t2");
        assert_eq!(labels(&rule)[3].value, "p2");
        assert_eq!(rule.index, RULE_INDEX_PARTITION);
        assert_eq!(
            ranges(&rule)[0]["start_key"],
            "7480000000000000ff0200000000000000f8"
        );
        assert_eq!(
            ranges(&rule)[0]["end_key"],
            "7480000000000000ff0300000000000000f8"
        );

        // The DEFAULT spec resets to an ID-only rule.
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&default_spec()).unwrap();
        rule.reset(&CodecV1, "db3", "t3", "p3", &[3]);
        let mut expected = Rule::new();
        expected.id = "schema/db3/t3/p3".to_owned();
        expected.labels = Vec::new().into();
        assert_eq!(rule, expected);
    }

    // Go `TestResetWithKeyspaceCodec`, both build-tag arms.
    #[test]
    fn keyspace_codec_matches_the_selected_kernel() {
        let codec =
            tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, 42).unwrap();
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&spec("key=value")).unwrap();
        rule.reset(&codec, "db1", "t1", "", &[1]);
        if tidb_config::kerneltype::is_next_gen() {
            assert_eq!(rule.id, "keyspace/42/schema/db1/t1");
            assert!(labels(&rule).contains(&RegionLabel {
                key: KEYSPACE_KEY.to_owned(),
                value: "42".to_owned(),
                ..RegionLabel::default()
            }));
            let (start, end) =
                codec.encode_region_range(&gen_table_prefix(1), &gen_table_prefix(2));
            assert_eq!(ranges(&rule)[0]["start_key"], hex_lower(&start));
            assert_eq!(ranges(&rule)[0]["end_key"], hex_lower(&end));
        } else {
            assert_eq!(rule.id, "schema/db1/t1");
            assert_eq!(labels(&rule).len(), 3);
            assert_eq!(
                ranges(&rule)[0]["start_key"],
                "7480000000000000ff0100000000000000f8"
            );
        }
        assert_eq!(restore_rule_id(&rule.id), "schema/db1/t1");
    }

    // Go `NewRulePatch`.
    #[test]
    fn rule_patches_carry_sets_and_deletes() {
        let mut rule = Rule::new();
        rule.apply_attributes_spec(&spec("key=value")).unwrap();
        rule.reset(&CodecV1, "db", "t", "", &[1]);
        let patch = new_rule_patch(vec![rule.clone()], vec!["schema/db/gone".to_owned()]);
        assert_eq!(patch.set_rules.snapshot(), vec![rule]);
        assert_eq!(
            patch.delete_rules.snapshot(),
            vec!["schema/db/gone".to_owned()]
        );

        let wire = serde_json::to_value(&patch).expect("PD patch JSON");
        assert_eq!(wire["sets"][0]["id"], "schema/db/t");
        assert_eq!(wire["sets"][0]["rule_type"], "key-range");
        assert_eq!(wire["deletes"][0], "schema/db/gone");
        assert!(wire.get("set_rules").is_none());
        assert!(wire.get("delete_rules").is_none());
        assert_eq!(
            serde_json::from_value::<LabelRulePatch>(wire).expect("PD patch decodes"),
            patch
        );

        let zero = serde_json::to_value(LabelRulePatch::default()).unwrap();
        assert_eq!(zero, serde_json::json!({"sets": null, "deletes": null}));
        let allocated = serde_json::to_value(new_rule_patch(Vec::new(), Vec::new())).unwrap();
        assert_eq!(allocated, serde_json::json!({"sets": [], "deletes": []}));
    }

    #[test]
    fn rule_string_matches_go_zero_value_json() {
        assert_eq!(
            Rule::new().to_string(),
            r#"{"id":"","index":0,"labels":null,"rule_type":"","data":null}"#
        );
    }

    #[test]
    fn clone_copies_the_go_slice_header_shallowly() {
        let mut original = Rule::new();
        original
            .apply_attributes_spec(&spec("db=old,table=old"))
            .unwrap();
        let mut cloned = original.clone_rule();

        cloned.reset(&CodecV1, "new_db", "new_table", "", &[1]);

        assert_eq!(labels(&original)[0].value, "new_db");
        assert_eq!(labels(&original)[1].value, "new_table");
        assert!(original.labels.backing_ptr_eq(&cloned.labels));

        let data_alias = cloned.clone_rule();
        data_alias.data.as_ref().expect("Reset writes Data").write()[0]["start_key"] =
            serde_json::Value::String("changed".to_owned());
        assert_eq!(ranges(&cloned)[0]["start_key"], "changed");
        assert_eq!(ranges(&data_alias)[0]["start_key"], "changed");
    }

    #[test]
    fn pd_rule_wire_preserves_expiry_and_arbitrary_data() {
        let wire = serde_json::json!({
            "id": "external",
            "index": 7,
            "labels": [{
                "key": "zone",
                "value": "z1",
                "ttl": "10m",
                "start_at": "2026-08-31T00:00:00Z"
            }],
            "rule_type": "custom",
            "data": {"opaque": [1, true, "x"]}
        });
        let rule: Rule = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(rule).unwrap(), wire);
    }
}
