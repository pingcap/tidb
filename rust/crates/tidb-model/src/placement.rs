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
///
/// Go embeds `*PlacementSettings`; here it is a named `Option<Box<..>>`
/// field to keep the nil-able pointer, deep-cloned by the derived `Clone`.
///
/// The embedded field has no JSON tag, so Go promotes the settings into this
/// object; `flatten` reproduces that. One divergence: Go decodes an object with
/// no settings keys into a nil pointer, while a flattened `Option` yields
/// `Some(PlacementSettings::default())`, which then re-serializes the twelve
/// zero-valued settings. Policies stored by TiDB always carry settings.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PolicyInfo {
    /// The placement settings (Go's embedded `*PlacementSettings`).
    #[serde(flatten)]
    pub placement_settings: Option<Box<PlacementSettings>>,
    /// The policy ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The policy name.
    #[serde(rename = "name", default)]
    pub name: CiString,
    /// The online-DDL state of the policy object.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
}

#[cfg(test)]
mod tests {
    use super::*;

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
