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

//! `pkg/meta/model/masking_policy.go`: the masking-policy metadata.

use chrono::{DateTime, Datelike, FixedOffset, TimeZone, Timelike};
use tidb_ast::{CiString, MaskingPolicyRestrictOps};

use crate::schema_state::SchemaState;
use crate::serde_helpers::go_json_field_matches;

/// Go `MaskingPolicyStatus` (a `byte`): whether a masking policy is active.
/// A newtype over `u8` so unknown stored values round-trip and `Display`
/// yields `""` for them, matching Go's `switch` default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MaskingPolicyStatus(pub u8);

impl MaskingPolicyStatus {
    /// The policy is disabled (Go `MaskingPolicyStatusDisable`, zero value).
    pub const DISABLE: MaskingPolicyStatus = MaskingPolicyStatus(0);
    /// The policy is enabled (Go `MaskingPolicyStatusEnable`).
    pub const ENABLE: MaskingPolicyStatus = MaskingPolicyStatus(1);
    /// Compatibility alias for [`DISABLE`](Self::DISABLE)
    /// (Go `MaskingPolicyStatusDisabled`).
    pub const DISABLED: MaskingPolicyStatus = Self::DISABLE;
    /// Compatibility alias for [`ENABLE`](Self::ENABLE)
    /// (Go `MaskingPolicyStatusEnabled`).
    pub const ENABLED: MaskingPolicyStatus = Self::ENABLE;
}

impl std::fmt::Display for MaskingPolicyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            MaskingPolicyStatus::DISABLE => "DISABLED",
            MaskingPolicyStatus::ENABLE => "ENABLED",
            _ => "",
        })
    }
}

/// Go `MaskingPolicyType`, an open named string that preserves unknown values.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
enum MaskingPolicyTypeValue {
    #[default]
    Empty,
    Full,
    Partial,
    Null,
    Date,
    Custom,
    Other(String),
}

/// Go `MaskingPolicyType`, with typed known constants and an open string
/// representation for values written by newer implementations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct MaskingPolicyType(MaskingPolicyTypeValue);

impl MaskingPolicyType {
    /// Go `MaskingPolicyTypeFull`.
    pub const FULL: Self = Self(MaskingPolicyTypeValue::Full);
    /// Go `MaskingPolicyTypePartial`.
    pub const PARTIAL: Self = Self(MaskingPolicyTypeValue::Partial);
    /// Go `MaskingPolicyTypeNull`.
    pub const NULL: Self = Self(MaskingPolicyTypeValue::Null);
    /// Go `MaskingPolicyTypeDate`.
    pub const DATE: Self = Self(MaskingPolicyTypeValue::Date);
    /// Go `MaskingPolicyTypeCustom`.
    pub const CUSTOM: Self = Self(MaskingPolicyTypeValue::Custom);

    /// Compatibility alias for Go `MaskingPolicyTypeMaskFull`.
    pub const MASK_FULL: Self = Self::FULL;
    /// Compatibility alias for Go `MaskingPolicyTypeMaskPartial`.
    pub const MASK_PARTIAL: Self = Self::PARTIAL;
    /// Compatibility alias for Go `MaskingPolicyTypeMaskNull`.
    pub const MASK_NULL: Self = Self::NULL;
    /// Compatibility alias for Go `MaskingPolicyTypeMaskDate`.
    pub const MASK_DATE: Self = Self::DATE;

    /// Constructs a source-compatible open string value.
    pub fn new(value: impl Into<String>) -> Self {
        match value.into() {
            value if value.is_empty() => Self::default(),
            value if value == "MASK_FULL" => Self::FULL,
            value if value == "MASK_PARTIAL" => Self::PARTIAL,
            value if value == "MASK_NULL" => Self::NULL,
            value if value == "MASK_DATE" => Self::DATE,
            value if value == "CUSTOM" => Self::CUSTOM,
            value => Self(MaskingPolicyTypeValue::Other(value)),
        }
    }

    /// Borrows the exact stored spelling.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            MaskingPolicyTypeValue::Empty => "",
            MaskingPolicyTypeValue::Full => "MASK_FULL",
            MaskingPolicyTypeValue::Partial => "MASK_PARTIAL",
            MaskingPolicyTypeValue::Null => "MASK_NULL",
            MaskingPolicyTypeValue::Date => "MASK_DATE",
            MaskingPolicyTypeValue::Custom => "CUSTOM",
            MaskingPolicyTypeValue::Other(value) => value,
        }
    }

    fn is_empty(&self) -> bool {
        matches!(&self.0, MaskingPolicyTypeValue::Empty)
    }
}

impl From<&str> for MaskingPolicyType {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for MaskingPolicyType {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for MaskingPolicyType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl serde::Serialize for MaskingPolicyType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for MaskingPolicyType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(<String as serde::Deserialize>::deserialize(
            deserializer,
        )?))
    }
}

fn go_zero_time() -> DateTime<FixedOffset> {
    FixedOffset::east_opt(0)
        .expect("UTC offset exists")
        .with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("Go zero time is representable")
}

fn restrict_ops_is_empty(value: &MaskingPolicyRestrictOps) -> bool {
    value.is_empty()
}

fn format_go_time(value: &DateTime<FixedOffset>) -> Result<String, &'static str> {
    if !(0..=9999).contains(&value.year()) {
        return Err("Time.MarshalJSON: year outside of range [0,9999]");
    }
    let offset = value.offset().local_minus_utc();
    if offset % 60 != 0 {
        return Err("Time.MarshalJSON: timezone offset has seconds");
    }

    let mut encoded = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    if value.nanosecond() != 0 {
        let mut fraction = format!("{:09}", value.nanosecond());
        while fraction.ends_with('0') {
            fraction.pop();
        }
        encoded.push('.');
        encoded.push_str(&fraction);
    }
    if offset == 0 {
        encoded.push('Z');
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let absolute = offset.unsigned_abs();
        encoded.push(sign);
        encoded.push_str(&format!(
            "{:02}:{:02}",
            absolute / 3600,
            (absolute / 60) % 60
        ));
    }
    Ok(encoded)
}

mod go_time_serde {
    use super::{format_go_time, DateTime, FixedOffset};
    use serde::ser::Error as _;

    pub fn serialize<S: serde::Serializer>(
        value: &DateTime<FixedOffset>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_go_time(value).map_err(S::Error::custom)?)
    }
}

/// Go `MaskingPolicyInfo`: the stored definition of a column masking policy.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct MaskingPolicyInfo {
    /// The policy ID.
    pub id: i64,
    /// The policy name.
    pub name: CiString,
    /// The database the target column lives in.
    pub db_name: CiString,
    /// The target table name.
    pub table_name: CiString,
    /// The target table ID.
    pub table_id: i64,
    /// The target column name.
    pub column_name: CiString,
    /// The target column ID.
    pub column_id: i64,
    /// The masking expression.
    pub expression: String,
    /// Whether the policy is enabled.
    pub status: MaskingPolicyStatus,
    /// The masking type.
    #[serde(skip_serializing_if = "MaskingPolicyType::is_empty")]
    pub masking_type: MaskingPolicyType,
    /// The operations the policy restricts.
    #[serde(skip_serializing_if = "restrict_ops_is_empty")]
    pub restrict_ops: MaskingPolicyRestrictOps,
    /// When the policy was created.
    #[serde(serialize_with = "go_time_serde::serialize")]
    pub created_at: DateTime<FixedOffset>,
    /// When the policy was last updated.
    #[serde(serialize_with = "go_time_serde::serialize")]
    pub updated_at: DateTime<FixedOffset>,
    /// Who created the policy.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub created_by: String,
    /// Who last updated the policy.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub updated_by: String,
    /// The online-DDL state of the policy object.
    pub state: SchemaState,
}

impl Default for MaskingPolicyInfo {
    fn default() -> Self {
        Self {
            id: 0,
            name: CiString::default(),
            db_name: CiString::default(),
            table_name: CiString::default(),
            table_id: 0,
            column_name: CiString::default(),
            column_id: 0,
            expression: String::new(),
            status: MaskingPolicyStatus::default(),
            masking_type: MaskingPolicyType::default(),
            restrict_ops: MaskingPolicyRestrictOps::default(),
            created_at: go_zero_time(),
            updated_at: go_zero_time(),
            created_by: String::new(),
            updated_by: String::new(),
            state: SchemaState::default(),
        }
    }
}

impl<'de> serde::Deserialize<'de> for MaskingPolicyInfo {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MaskingPolicyInfoVisitor;

        impl<'de> serde::de::Visitor<'de> for MaskingPolicyInfoVisitor {
            type Value = MaskingPolicyInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Go MaskingPolicyInfo JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut policy = MaskingPolicyInfo::default();
                while let Some(key) = map.next_key::<String>()? {
                    if go_json_field_matches(&key, "id") {
                        if let Some(value) = map.next_value::<Option<i64>>()? {
                            policy.id = value;
                        }
                    } else if go_json_field_matches(&key, "name") {
                        if let Some(value) = map.next_value::<Option<CiString>>()? {
                            policy.name = value;
                        }
                    } else if go_json_field_matches(&key, "db_name") {
                        if let Some(value) = map.next_value::<Option<CiString>>()? {
                            policy.db_name = value;
                        }
                    } else if go_json_field_matches(&key, "table_name") {
                        if let Some(value) = map.next_value::<Option<CiString>>()? {
                            policy.table_name = value;
                        }
                    } else if go_json_field_matches(&key, "table_id") {
                        if let Some(value) = map.next_value::<Option<i64>>()? {
                            policy.table_id = value;
                        }
                    } else if go_json_field_matches(&key, "column_name") {
                        if let Some(value) = map.next_value::<Option<CiString>>()? {
                            policy.column_name = value;
                        }
                    } else if go_json_field_matches(&key, "column_id") {
                        if let Some(value) = map.next_value::<Option<i64>>()? {
                            policy.column_id = value;
                        }
                    } else if go_json_field_matches(&key, "expression") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            policy.expression = value;
                        }
                    } else if go_json_field_matches(&key, "status") {
                        if let Some(value) = map.next_value::<Option<MaskingPolicyStatus>>()? {
                            policy.status = value;
                        }
                    } else if go_json_field_matches(&key, "masking_type") {
                        if let Some(value) = map.next_value::<Option<MaskingPolicyType>>()? {
                            policy.masking_type = value;
                        }
                    } else if go_json_field_matches(&key, "restrict_ops") {
                        if let Some(value) = map.next_value::<Option<MaskingPolicyRestrictOps>>()? {
                            policy.restrict_ops = value;
                        }
                    } else if go_json_field_matches(&key, "created_at") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            policy.created_at = DateTime::parse_from_rfc3339(&value)
                                .map_err(serde::de::Error::custom)?;
                        }
                    } else if go_json_field_matches(&key, "updated_at") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            policy.updated_at = DateTime::parse_from_rfc3339(&value)
                                .map_err(serde::de::Error::custom)?;
                        }
                    } else if go_json_field_matches(&key, "created_by") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            policy.created_by = value;
                        }
                    } else if go_json_field_matches(&key, "updated_by") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            policy.updated_by = value;
                        }
                    } else if go_json_field_matches(&key, "state") {
                        if let Some(value) = map.next_value::<Option<SchemaState>>()? {
                            policy.state = value;
                        }
                    } else {
                        let _ = map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(policy)
            }
        }

        deserializer.deserialize_map(MaskingPolicyInfoVisitor)
    }
}

/// Rust's explicit equivalent of Go's nil-safe `(*MaskingPolicyInfo).Clone`.
pub fn clone_masking_policy_info(policy: Option<&MaskingPolicyInfo>) -> Option<MaskingPolicyInfo> {
    policy.cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_helpers::to_go_json;

    // Go's MaskingPolicyStatus.String() switch, plus the "" default and the
    // compatibility aliases.
    #[test]
    fn status_strings() {
        assert_eq!(MaskingPolicyStatus::DISABLE.to_string(), "DISABLED");
        assert_eq!(MaskingPolicyStatus::ENABLE.to_string(), "ENABLED");
        assert_eq!(MaskingPolicyStatus(9).to_string(), "");
        assert_eq!(MaskingPolicyStatus::default(), MaskingPolicyStatus::DISABLE);
        assert_eq!(MaskingPolicyStatus::DISABLED, MaskingPolicyStatus::DISABLE);
        assert_eq!(MaskingPolicyStatus::ENABLED, MaskingPolicyStatus::ENABLE);
        assert!(serde_json::from_str::<MaskingPolicyStatus>("256").is_err());
    }

    #[test]
    fn type_values() {
        assert_eq!(MaskingPolicyType::FULL.as_str(), "MASK_FULL");
        assert_eq!(MaskingPolicyType::PARTIAL.as_str(), "MASK_PARTIAL");
        assert_eq!(MaskingPolicyType::NULL.as_str(), "MASK_NULL");
        assert_eq!(MaskingPolicyType::DATE.as_str(), "MASK_DATE");
        assert_eq!(MaskingPolicyType::CUSTOM.as_str(), "CUSTOM");
        assert_eq!(MaskingPolicyType::MASK_FULL, MaskingPolicyType::FULL);
        assert_eq!(MaskingPolicyType::MASK_PARTIAL, MaskingPolicyType::PARTIAL);
        assert_eq!(MaskingPolicyType::MASK_NULL, MaskingPolicyType::NULL);
        assert_eq!(MaskingPolicyType::MASK_DATE, MaskingPolicyType::DATE);
        assert_eq!(MaskingPolicyType::from("future").as_str(), "future");
    }

    #[test]
    fn zero_json_matches_go_and_null_time_decodes_to_zero() {
        const GO_ZERO: &str = r#"{"id":0,"name":{"O":"","L":""},"db_name":{"O":"","L":""},"table_name":{"O":"","L":""},"table_id":0,"column_name":{"O":"","L":""},"column_id":0,"expression":"","status":0,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z","state":0}"#;
        let zero = MaskingPolicyInfo::default();
        assert_eq!(
            String::from_utf8(to_go_json(&zero).unwrap()).unwrap(),
            GO_ZERO
        );

        let missing: MaskingPolicyInfo = serde_json::from_str("{}").unwrap();
        assert_eq!(missing, zero);
        let null_times: MaskingPolicyInfo =
            serde_json::from_str(r#"{"created_at":null,"updated_at":null}"#).unwrap();
        assert_eq!(null_times, zero);

        let all_null: MaskingPolicyInfo = serde_json::from_str(
            r#"{"id":null,"name":null,"db_name":null,"table_name":null,"table_id":null,"column_name":null,"column_id":null,"expression":null,"status":null,"masking_type":null,"restrict_ops":null,"created_at":null,"updated_at":null,"created_by":null,"updated_by":null,"state":null}"#,
        )
        .unwrap();
        assert_eq!(all_null, zero);

        let duplicate_case: MaskingPolicyInfo = serde_json::from_str(
            r#"{"ID":1,"id":2,"STATUS":1,"status":null,"MASKING_TYPE":"CUSTOM","masking_type":"future","TABLE_ID":7,"TableID":8,"NAME":{"O":"A","L":"a"},"expreſsion":"folded"}"#,
        )
        .unwrap();
        assert_eq!(duplicate_case.id, 2);
        assert_eq!(duplicate_case.status, MaskingPolicyStatus::ENABLE);
        assert_eq!(duplicate_case.masking_type.as_str(), "future");
        assert_eq!(duplicate_case.table_id, 7);
        assert_eq!(duplicate_case.name.original(), "A");
        assert_eq!(duplicate_case.expression, "folded");
    }

    #[test]
    fn full_json_matches_go_at_every_field_boundary() {
        let plus_eight = FixedOffset::east_opt(8 * 60 * 60).unwrap();
        let updated_at = plus_eight
            .with_ymd_and_hms(2026, 8, 7, 1, 2, 3)
            .single()
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();
        let policy = MaskingPolicyInfo {
            id: i64::MIN,
            name: CiString::new("PoLiCy"),
            db_name: CiString::new("DB"),
            table_name: CiString::new("T"),
            table_id: i64::MAX,
            column_name: CiString::new("C"),
            column_id: -7,
            expression: "x < y & \u{2028}".to_owned(),
            status: MaskingPolicyStatus::ENABLED,
            masking_type: MaskingPolicyType::MASK_FULL,
            restrict_ops: MaskingPolicyRestrictOps::from_bits(u64::MAX),
            created_at: go_zero_time(),
            updated_at,
            created_by: "creator".to_owned(),
            updated_by: "updater".to_owned(),
            state: SchemaState::PUBLIC,
        };
        const GO_FULL: &str = r#"{"id":-9223372036854775808,"name":{"O":"PoLiCy","L":"policy"},"db_name":{"O":"DB","L":"db"},"table_name":{"O":"T","L":"t"},"table_id":9223372036854775807,"column_name":{"O":"C","L":"c"},"column_id":-7,"expression":"x \u003c y \u0026 \u2028","status":1,"masking_type":"MASK_FULL","restrict_ops":18446744073709551615,"created_at":"0001-01-01T00:00:00Z","updated_at":"2026-08-07T01:02:03.123456789+08:00","created_by":"creator","updated_by":"updater","state":5}"#;
        assert_eq!(
            String::from_utf8(to_go_json(&policy).unwrap()).unwrap(),
            GO_FULL
        );

        let decoded: MaskingPolicyInfo = serde_json::from_str(GO_FULL).unwrap();
        assert_eq!(decoded, policy);
        assert_eq!(
            serde_json::to_string(&MaskingPolicyStatus(255)).unwrap(),
            "255"
        );
        assert_eq!(
            serde_json::to_string(&MaskingPolicyType::from("future")).unwrap(),
            r#""future""#
        );
    }

    #[test]
    fn clone_matches_value_copy_and_nil_receiver() {
        assert_eq!(clone_masking_policy_info(None), None);
        let original = MaskingPolicyInfo {
            name: CiString::new("PoLiCy"),
            expression: "mask(c)".to_owned(),
            ..MaskingPolicyInfo::default()
        };
        let mut cloned = clone_masking_policy_info(Some(&original)).unwrap();
        assert_eq!(cloned, original);
        cloned.expression.push_str(" changed");
        assert_eq!(original.expression, "mask(c)");
    }
}
