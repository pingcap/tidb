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

//! `pkg/meta/model/placement.go`: placement-policy metadata.
//!
//! The `writeSetting*ToBuilder` helpers live in [`crate::setting_builder`],
//! shared with the resource-group renderer.

use serde::Serialize;
use tidb_ast::CiString;

use crate::go_runtime::GoShared;
use crate::schema_state::SchemaState;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    FatalSeed, NullNoopSeed, ValueMergeSeed,
};
use crate::setting_builder::{write_setting_integer, write_setting_string};

/// Go `PolicyRefInfo`: a reference to a placement policy by ID and name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PolicyRefInfo {
    /// The policy ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The policy name.
    #[serde(rename = "name", default)]
    pub name: CiString,
}

impl_go_json_merge_object!(PolicyRefInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(PolicyRefInfo);

/// Go `PlacementSettings`: the placement configuration of a schema object.
///
/// No field carries `omitempty`, so every one is always written; the string
/// settings are plain JSON strings, not the `SHOW`-style rendering produced by
/// [`Display`](std::fmt::Display).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PlacementSettings {
    /// The primary region.
    #[serde(
        rename = "primary_region",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub primary_region: String,
    /// The regions.
    #[serde(
        rename = "regions",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub regions: String,
    /// The number of learner replicas.
    #[serde(rename = "learners", default)]
    pub learners: u64,
    /// The number of follower replicas.
    #[serde(rename = "followers", default)]
    pub followers: u64,
    /// The number of voter replicas.
    #[serde(rename = "voters", default)]
    pub voters: u64,
    /// The schedule policy.
    #[serde(
        rename = "schedule",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub schedule: String,
    /// The replica constraints.
    #[serde(
        rename = "constraints",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub constraints: String,
    /// The leader constraints.
    #[serde(
        rename = "leader_constraints",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub leader_constraints: String,
    /// The learner constraints.
    #[serde(
        rename = "learner_constraints",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub learner_constraints: String,
    /// The follower constraints.
    #[serde(
        rename = "follower_constraints",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub follower_constraints: String,
    /// The voter constraints.
    #[serde(
        rename = "voter_constraints",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub voter_constraints: String,
    /// The survival preferences.
    #[serde(
        rename = "survival_preferences",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub survival_preferences: String,
}

impl PlacementSettings {
    /// Go `PlacementSettings.String` (`meta/model/placement.go`): the clause
    /// `SHOW CREATE PLACEMENT POLICY` prints after the policy name.
    ///
    /// The EMISSION ORDER is Go's and is not the struct's field order:
    /// primary region, regions, schedule, constraints, leader constraints,
    /// then voters/voter constraints, followers/follower constraints,
    /// learners/learner constraints, and finally survival preferences. Each
    /// item is emitted only when it is set -- a zero count and an empty
    /// string are both "unset" -- and items are separated by ONE space.
    ///
    /// A string value is wrapped in double quotes with any embedded double
    /// quote backslash-escaped (`writeSettingStringToBuilder`); an integer is
    /// written bare (`writeSettingIntegerToBuilder`).
    #[must_use]
    pub fn to_clause(&self) -> String {
        let mut out = String::new();
        let mut push_string = |out: &mut String, item: &str, value: &str| {
            if value.is_empty() {
                return;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(&format!("{item}=\"{}\"", value.replace('"', "\\\"")));
        };
        push_string(&mut out, "PRIMARY_REGION", &self.primary_region);
        push_string(&mut out, "REGIONS", &self.regions);
        push_string(&mut out, "SCHEDULE", &self.schedule);
        push_string(&mut out, "CONSTRAINTS", &self.constraints);
        push_string(&mut out, "LEADER_CONSTRAINTS", &self.leader_constraints);
        let mut push_int = |out: &mut String, item: &str, value: u64| {
            if value == 0 {
                return;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(&format!("{item}={value}"));
        };
        push_int(&mut out, "VOTERS", self.voters);
        push_string(&mut out, "VOTER_CONSTRAINTS", &self.voter_constraints);
        push_int(&mut out, "FOLLOWERS", self.followers);
        push_string(&mut out, "FOLLOWER_CONSTRAINTS", &self.follower_constraints);
        push_int(&mut out, "LEARNERS", self.learners);
        push_string(&mut out, "LEARNER_CONSTRAINTS", &self.learner_constraints);
        push_string(&mut out, "SURVIVAL_PREFERENCES", &self.survival_preferences);
        out
    }
}

impl_go_json_merge_object!(PlacementSettings, destination, map, key, {
    if go_json_field_matches(&key, "primary_region") {
        map.next_value_seed(NullNoopSeed(&mut destination.primary_region))?;
    } else if go_json_field_matches(&key, "regions") {
        map.next_value_seed(NullNoopSeed(&mut destination.regions))?;
    } else if go_json_field_matches(&key, "learners") {
        map.next_value_seed(NullNoopSeed(&mut destination.learners))?;
    } else if go_json_field_matches(&key, "followers") {
        map.next_value_seed(NullNoopSeed(&mut destination.followers))?;
    } else if go_json_field_matches(&key, "voters") {
        map.next_value_seed(NullNoopSeed(&mut destination.voters))?;
    } else if go_json_field_matches(&key, "schedule") {
        map.next_value_seed(NullNoopSeed(&mut destination.schedule))?;
    } else if go_json_field_matches(&key, "constraints") {
        map.next_value_seed(NullNoopSeed(&mut destination.constraints))?;
    } else if go_json_field_matches(&key, "leader_constraints") {
        map.next_value_seed(NullNoopSeed(&mut destination.leader_constraints))?;
    } else if go_json_field_matches(&key, "learner_constraints") {
        map.next_value_seed(NullNoopSeed(&mut destination.learner_constraints))?;
    } else if go_json_field_matches(&key, "follower_constraints") {
        map.next_value_seed(NullNoopSeed(&mut destination.follower_constraints))?;
    } else if go_json_field_matches(&key, "voter_constraints") {
        map.next_value_seed(NullNoopSeed(&mut destination.voter_constraints))?;
    } else if go_json_field_matches(&key, "survival_preferences") {
        map.next_value_seed(NullNoopSeed(&mut destination.survival_preferences))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(PlacementSettings);

impl std::fmt::Display for PlacementSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reproduces Go's PlacementSettings.String() field order and format.
        let mut sb = String::new();
        if !self.primary_region.is_empty() {
            write_setting_string(&mut sb, "PRIMARY_REGION", &self.primary_region, None);
        }
        if !self.regions.is_empty() {
            write_setting_string(&mut sb, "REGIONS", &self.regions, None);
        }
        if !self.schedule.is_empty() {
            write_setting_string(&mut sb, "SCHEDULE", &self.schedule, None);
        }
        if !self.constraints.is_empty() {
            write_setting_string(&mut sb, "CONSTRAINTS", &self.constraints, None);
        }
        if !self.leader_constraints.is_empty() {
            write_setting_string(
                &mut sb,
                "LEADER_CONSTRAINTS",
                &self.leader_constraints,
                None,
            );
        }
        if self.voters > 0 {
            write_setting_integer(&mut sb, "VOTERS", self.voters, None);
        }
        if !self.voter_constraints.is_empty() {
            write_setting_string(&mut sb, "VOTER_CONSTRAINTS", &self.voter_constraints, None);
        }
        if self.followers > 0 {
            write_setting_integer(&mut sb, "FOLLOWERS", self.followers, None);
        }
        if !self.follower_constraints.is_empty() {
            write_setting_string(
                &mut sb,
                "FOLLOWER_CONSTRAINTS",
                &self.follower_constraints,
                None,
            );
        }
        if self.learners > 0 {
            write_setting_integer(&mut sb, "LEARNERS", self.learners, None);
        }
        if !self.learner_constraints.is_empty() {
            write_setting_string(
                &mut sb,
                "LEARNER_CONSTRAINTS",
                &self.learner_constraints,
                None,
            );
        }
        if !self.survival_preferences.is_empty() {
            write_setting_string(
                &mut sb,
                "SURVIVAL_PREFERENCES",
                &self.survival_preferences,
                None,
            );
        }
        f.write_str(&sb)
    }
}

impl PlacementSettings {
    /// Pointer-shaped Go `(*PlacementSettings).Clone` boundary.
    #[must_use]
    pub fn clone_pointer(settings: Option<&Self>) -> GoShared<Self> {
        GoShared::new(settings.expect("nil *PlacementSettings").clone())
    }
}

/// Go `PolicyInfo`: a placement policy (its settings plus identity/state).
/// The embedded settings pointer is encoded as promoted fields and remains
/// nil when none of those fields occur during decode.
#[derive(Clone, Debug, Default)]
pub struct PolicyInfo {
    /// The placement settings (Go's embedded `*PlacementSettings`).
    pub placement_settings: Option<GoShared<PlacementSettings>>,
    /// The policy ID.
    pub id: i64,
    /// The policy name.
    pub name: CiString,
    /// The online-DDL state of the policy object.
    pub state: SchemaState,
}

impl Serialize for PolicyInfo {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;
        if let Some(settings) = &self.placement_settings {
            let settings = settings.read();
            map.serialize_entry("primary_region", &settings.primary_region)?;
            map.serialize_entry("regions", &settings.regions)?;
            map.serialize_entry("learners", &settings.learners)?;
            map.serialize_entry("followers", &settings.followers)?;
            map.serialize_entry("voters", &settings.voters)?;
            map.serialize_entry("schedule", &settings.schedule)?;
            map.serialize_entry("constraints", &settings.constraints)?;
            map.serialize_entry("leader_constraints", &settings.leader_constraints)?;
            map.serialize_entry("learner_constraints", &settings.learner_constraints)?;
            map.serialize_entry("follower_constraints", &settings.follower_constraints)?;
            map.serialize_entry("voter_constraints", &settings.voter_constraints)?;
            map.serialize_entry("survival_preferences", &settings.survival_preferences)?;
        }
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("state", &self.state)?;
        map.end()
    }
}

impl_go_json_merge_object!(PolicyInfo, destination, map, key, {
    if go_json_field_matches(&key, "primary_region") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.primary_region))?;
    } else if go_json_field_matches(&key, "regions") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.regions))?;
    } else if go_json_field_matches(&key, "learners") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.learners))?;
    } else if go_json_field_matches(&key, "followers") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.followers))?;
    } else if go_json_field_matches(&key, "voters") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.voters))?;
    } else if go_json_field_matches(&key, "schedule") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.schedule))?;
    } else if go_json_field_matches(&key, "constraints") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.constraints))?;
    } else if go_json_field_matches(&key, "leader_constraints") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.leader_constraints))?;
    } else if go_json_field_matches(&key, "learner_constraints") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.learner_constraints))?;
    } else if go_json_field_matches(&key, "follower_constraints") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.follower_constraints))?;
    } else if go_json_field_matches(&key, "voter_constraints") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.voter_constraints))?;
    } else if go_json_field_matches(&key, "survival_preferences") {
        let settings = destination
            .placement_settings
            .get_or_insert_with(|| GoShared::new(PlacementSettings::default()));
        let mut settings = settings.write();
        map.next_value_seed(NullNoopSeed(&mut settings.survival_preferences))?;
    } else if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(PolicyInfo);

impl PolicyInfo {
    /// Go `PolicyInfo.Clone`. The source dereferences the embedded settings
    /// pointer unconditionally, so a nil settings pointer is an invariant
    /// violation and panics rather than silently producing another nil.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        Self {
            placement_settings: Some(GoShared::new(
                self.placement_settings
                    .as_ref()
                    .expect("nil PlacementSettings in PolicyInfo.Clone")
                    .read()
                    .clone(),
            )),
            id: self.id,
            name: self.name.clone(),
            state: self.state,
        }
    }

    /// Pointer-shaped Go `(*PolicyInfo).Clone` boundary.
    #[must_use]
    pub fn clone_pointer(policy: Option<&Self>) -> GoShared<Self> {
        GoShared::new(policy.expect("nil *PolicyInfo").clone_like_go())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_clone_deep_copies_settings() {
        let policy = PolicyInfo {
            placement_settings: Some(GoShared::new(PlacementSettings {
                primary_region: "r1".to_owned(),
                ..Default::default()
            })),
            id: 1,
            ..Default::default()
        };
        let structural = policy.clone();
        assert!(structural
            .placement_settings
            .as_ref()
            .unwrap()
            .ptr_eq(policy.placement_settings.as_ref().unwrap()));
        let clone = policy.clone_like_go();
        assert!(!clone
            .placement_settings
            .as_ref()
            .unwrap()
            .ptr_eq(policy.placement_settings.as_ref().unwrap()));
        clone
            .placement_settings
            .as_ref()
            .unwrap()
            .write()
            .primary_region = "r2".to_owned();
        assert_eq!(
            policy
                .placement_settings
                .as_ref()
                .unwrap()
                .read()
                .primary_region,
            "r1"
        );
    }

    #[test]
    fn policy_json_preserves_nil_embedded_settings() {
        let empty: PolicyInfo =
            serde_json::from_str(r#"{"id":1,"name":{"O":"p","L":"p"},"state":5}"#).unwrap();
        assert!(empty.placement_settings.is_none());
        assert_eq!(
            serde_json::to_string(&empty).unwrap(),
            r#"{"id":1,"name":{"O":"p","L":"p"},"state":5}"#
        );

        let with_settings: PolicyInfo = serde_json::from_str(
            r#"{"primary_region":"r1","id":1,"name":{"O":"p","L":"p"},"state":5}"#,
        )
        .unwrap();
        assert_eq!(
            with_settings
                .placement_settings
                .as_ref()
                .unwrap()
                .read()
                .primary_region,
            "r1"
        );

        // Any matched promoted field walks and allocates the embedded pointer
        // before decoding its value. A scalar null then leaves the zero field.
        let null_promoted: PolicyInfo =
            serde_json::from_str(r#"{"PRIMARY_REGION":null,"id":2}"#).unwrap();
        assert!(null_promoted.placement_settings.is_some());
        assert_eq!(
            null_promoted
                .placement_settings
                .as_ref()
                .unwrap()
                .read()
                .primary_region,
            ""
        );

        // `encoding/json` uses Unicode SimpleFold for promoted field names.
        // This allocation must be decided here in the model decoder; a
        // post-decode exact-spelling scan drops valid metadata.
        let folded: PolicyInfo = serde_json::from_str(r#"{"PRIMARY_REGION":"r2","ID":3}"#).unwrap();
        assert_eq!(
            folded
                .placement_settings
                .as_ref()
                .unwrap()
                .read()
                .primary_region,
            "r2"
        );
    }

    #[test]
    fn placement_decode_keeps_first_error_and_applies_later_members() {
        use crate::serde_helpers::GoJsonMerge;

        let mut settings = PlacementSettings {
            voters: 7,
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"voters":null,"learners":"bad","LEARNERS":2,"REGIONS":"later"}"#,
        );
        assert!(settings.go_json_merge(&mut decoder).is_err());
        assert_eq!(settings.voters, 7);
        assert_eq!(settings.learners, 2);
        assert_eq!(settings.regions, "later");

        let mut policy = PolicyInfo {
            id: 9,
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"id":"bad","PRIMARY_REGION":null,"VOTERS":3,"ID":null}"#,
        );
        assert!(policy.go_json_merge(&mut decoder).is_err());
        assert_eq!(policy.id, 9);
        let promoted = policy.placement_settings.unwrap();
        let promoted = promoted.read();
        assert_eq!(promoted.primary_region, "");
        assert_eq!(promoted.voters, 3);

        let reference: PolicyRefInfo =
            serde_json::from_str(r#"{"id":7,"ID":null,"unknown":1}"#).unwrap();
        assert_eq!(reference.id, 7);
    }

    #[test]
    #[should_panic(expected = "nil PlacementSettings")]
    fn policy_clone_nil_settings_matches_source_invariant() {
        let _ = PolicyInfo::default().clone_like_go();
    }

    #[test]
    #[should_panic(expected = "nil *PolicyInfo")]
    fn policy_clone_nil_receiver_matches_source_dereference() {
        let _ = PolicyInfo::clone_pointer(None);
    }

    // Go TestPlacementSettingsString.
    #[test]
    fn placement_settings_string() {
        let s = PlacementSettings {
            primary_region: "us-east-1".into(),
            regions: "us-east-1,us-east-2".into(),
            schedule: "EVEN".into(),
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" SCHEDULE=\"EVEN\""
        );

        let s = PlacementSettings {
            leader_constraints: "[+region=bj]".into(),
            ..Default::default()
        };
        assert_eq!(s.to_string(), "LEADER_CONSTRAINTS=\"[+region=bj]\"");

        let s = PlacementSettings {
            voters: 1,
            voter_constraints: "[+region=us-east-1]".into(),
            followers: 2,
            follower_constraints: "[+disk=ssd]".into(),
            learners: 3,
            learner_constraints: "[+region=us-east-2]".into(),
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "VOTERS=1 VOTER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 \
             FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=us-east-2]\""
        );

        // Quote escaping.
        let s = PlacementSettings {
            voters: 3,
            followers: 2,
            learners: 1,
            constraints: "{\"+us-east-1\":1,+us-east-2:1}".into(),
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "CONSTRAINTS=\"{\\\"+us-east-1\\\":1,+us-east-2:1}\" VOTERS=3 FOLLOWERS=2 LEARNERS=1"
        );
    }

    // Go TestPlacementSettingsClone: mutating the clone leaves the original.
    #[test]
    fn placement_settings_clone() {
        let settings = PlacementSettings::default();
        let mut cloned = settings.clone();
        cloned.primary_region = "r1".into();
        cloned.followers = 3;
        cloned.constraints = "[+zone=z1]".into();
        assert_eq!(settings, PlacementSettings::default());
    }

    // Byte-compared against Go's json.Marshal of the same PolicyInfo.
    #[test]
    fn policy_info_json_matches_go() {
        let p = PolicyInfo {
            placement_settings: Some(GoShared::new(PlacementSettings {
                primary_region: "r".into(),
                followers: 2,
                ..Default::default()
            })),
            id: 7,
            name: CiString::new("pp"),
            state: SchemaState::PUBLIC,
        };
        let want = r#"{"primary_region":"r","regions":"","learners":0,"followers":2,"voters":0,"schedule":"","constraints":"","leader_constraints":"","learner_constraints":"","follower_constraints":"","voter_constraints":"","survival_preferences":"","id":7,"name":{"O":"pp","L":"pp"},"state":5}"#;
        assert_eq!(serde_json::to_string(&p).unwrap(), want);
        let back: PolicyInfo = serde_json::from_str(want).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), want);
    }

    // Go TestPlacementPolicyClone: the source method deep-copies the settings.
    #[test]
    fn policy_clone() {
        let policy = PolicyInfo {
            placement_settings: Some(GoShared::new(PlacementSettings::default())),
            ..Default::default()
        };
        let mut cloned = policy.clone_like_go();
        cloned.id = 100;
        cloned.name = CiString::new("p2");
        cloned.state = SchemaState::DELETE_ONLY;
        cloned
            .placement_settings
            .as_ref()
            .unwrap()
            .write()
            .followers = 10;

        assert_eq!(policy.id, 0);
        assert_eq!(policy.name, CiString::new(""));
        assert_eq!(policy.state, SchemaState::NONE);
        assert_eq!(
            *policy.placement_settings.as_ref().unwrap().read(),
            PlacementSettings::default()
        );
    }
}
