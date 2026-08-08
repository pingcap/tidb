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

use serde::{Deserialize, Serialize};
use tidb_ast::CiString;

use crate::schema_state::SchemaState;
use crate::setting_builder::{write_setting_integer, write_setting_string};

/// Go `PolicyRefInfo`: a reference to a placement policy by ID and name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRefInfo {
    /// The policy ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The policy name.
    #[serde(rename = "name", default)]
    pub name: CiString,
}

/// Go `PlacementSettings`: the placement configuration of a schema object.
///
/// No field carries `omitempty`, so every one is always written; the string
/// settings are plain JSON strings, not the `SHOW`-style rendering produced by
/// [`Display`](std::fmt::Display).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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

/// Go `PolicyInfo`: a placement policy (its settings plus identity/state).
/// The embedded settings pointer is encoded as promoted fields and remains
/// nil when none of those fields occur during decode.
#[derive(Clone, Debug, Default)]
pub struct PolicyInfo {
    /// The placement settings (Go's embedded `*PlacementSettings`).
    pub placement_settings: Option<Box<PlacementSettings>>,
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

impl<'de> Deserialize<'de> for PolicyInfo {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let mut object = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        let id = object
            .remove("id")
            .map_or(Ok(0), serde_json::from_value)
            .map_err(D::Error::custom)?;
        let name = object
            .remove("name")
            .map_or_else(|| Ok(CiString::default()), serde_json::from_value)
            .map_err(D::Error::custom)?;
        let state = object
            .remove("state")
            .map_or(Ok(SchemaState::default()), serde_json::from_value)
            .map_err(D::Error::custom)?;
        let settings_keys = [
            "primary_region",
            "regions",
            "learners",
            "followers",
            "voters",
            "schedule",
            "constraints",
            "leader_constraints",
            "learner_constraints",
            "follower_constraints",
            "voter_constraints",
            "survival_preferences",
        ];
        let has_settings = settings_keys.iter().any(|key| object.contains_key(*key));
        let placement_settings = if has_settings {
            let settings = serde_json::from_value(serde_json::Value::Object(object))
                .map_err(D::Error::custom)?;
            Some(Box::new(settings))
        } else {
            None
        };
        Ok(Self {
            placement_settings,
            id,
            name,
            state,
        })
    }
}

impl PolicyInfo {
    /// Go `PolicyInfo.Clone`. The source dereferences the embedded settings
    /// pointer unconditionally, so a nil settings pointer is an invariant
    /// violation and panics rather than silently producing another nil.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        Self {
            placement_settings: Some(Box::new(
                (**self
                    .placement_settings
                    .as_ref()
                    .expect("nil PlacementSettings in PolicyInfo.Clone"))
                .clone(),
            )),
            id: self.id,
            name: self.name.clone(),
            state: self.state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_clone_deep_copies_settings() {
        let policy = PolicyInfo {
            placement_settings: Some(Box::new(PlacementSettings {
                primary_region: "r1".to_owned(),
                ..Default::default()
            })),
            id: 1,
            ..Default::default()
        };
        let mut clone = policy.clone_like_go();
        clone.placement_settings.as_mut().unwrap().primary_region = "r2".to_owned();
        assert_eq!(
            policy.placement_settings.as_ref().unwrap().primary_region,
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
                .primary_region,
            "r1"
        );
    }

    #[test]
    #[should_panic(expected = "nil PlacementSettings")]
    fn policy_clone_nil_settings_matches_source_invariant() {
        let _ = PolicyInfo::default().clone_like_go();
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
            placement_settings: Some(Box::new(PlacementSettings {
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

    // Go TestPlacementPolicyClone: the clone deep-copies the settings box.
    #[test]
    fn policy_clone() {
        let policy = PolicyInfo {
            placement_settings: Some(Box::new(PlacementSettings::default())),
            ..Default::default()
        };
        let mut cloned = policy.clone();
        cloned.id = 100;
        cloned.name = CiString::new("p2");
        cloned.state = SchemaState::DELETE_ONLY;
        cloned.placement_settings.as_mut().unwrap().followers = 10;

        assert_eq!(policy.id, 0);
        assert_eq!(policy.name, CiString::new(""));
        assert_eq!(policy.state, SchemaState::NONE);
        assert_eq!(
            **policy.placement_settings.as_ref().unwrap(),
            PlacementSettings::default()
        );
    }
}
