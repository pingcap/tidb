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

use tidb_ast::CiString;

use crate::schema_state::SchemaState;
use crate::setting_builder::{write_setting_integer, write_setting_string};

/// Go `PolicyRefInfo`: a reference to a placement policy by ID and name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PolicyRefInfo {
    /// The policy ID.
    pub id: i64,
    /// The policy name.
    pub name: CiString,
}

/// Go `PlacementSettings`: the placement configuration of a schema object.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlacementSettings {
    /// The primary region.
    pub primary_region: String,
    /// The regions.
    pub regions: String,
    /// The number of learner replicas.
    pub learners: u64,
    /// The number of follower replicas.
    pub followers: u64,
    /// The number of voter replicas.
    pub voters: u64,
    /// The schedule policy.
    pub schedule: String,
    /// The replica constraints.
    pub constraints: String,
    /// The leader constraints.
    pub leader_constraints: String,
    /// The learner constraints.
    pub learner_constraints: String,
    /// The follower constraints.
    pub follower_constraints: String,
    /// The voter constraints.
    pub voter_constraints: String,
    /// The survival preferences.
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
