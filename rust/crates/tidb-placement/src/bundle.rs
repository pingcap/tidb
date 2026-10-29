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

//! Go `bundle.go`: a rule group, built from placement options and tidied into
//! the shape PD schedules best.

use std::fmt;
use tidb_hack::GoToLower;

use serde::Serialize;
use tidb_codec::encode_bytes;
use tidb_model::{
    GoShared, PartitionDefinition, PlacementSettings, PolicyInfo, PolicyRefInfo, TableInfo,
};
use tidb_tablecodec::table_key::gen_table_prefix;

use crate::common::{
    group_id, BUNDLE_ID_PREFIX, KEY_RANGE_GLOBAL, KEY_RANGE_META, META_PREFIX,
    RULE_INDEX_KEY_RANGE_FOR_GLOBAL, RULE_INDEX_KEY_RANGE_FOR_META, RULE_INDEX_PARTITION,
    RULE_INDEX_TABLE, TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL, TIDB_BUNDLE_RANGE_PREFIX_FOR_META,
};
use crate::constraint::new_constraint_direct;
use crate::constraints::{add_constraint, new_constraints_direct, new_constraints_from_yaml};
use crate::errors::{PlacementError, PlacementErrorKind};
use crate::pd::{LabelConstraintOp, PeerRoleType, Rule};
use crate::rule::{new_rule, RuleBuilder};
use crate::yaml_lite::unmarshal_strict_string_slice;

/// Go `Bundle`: a group of all rules and configurations. It is used to support
/// rule cache.
///
/// Go declares it as `type Bundle pd.GroupBundle`, aliasing the PD HTTP
/// client's group DTO to hang more methods off it; the four JSON fields are
/// that DTO's, so this struct is both (see [`crate::pd`]).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Bundle {
    /// The group ID, `TiDB_DDL_<object id>` for every TiDB-owned bundle.
    #[serde(rename = "group_id")]
    pub id: String,
    /// The group's scheduling priority.
    #[serde(rename = "group_index")]
    pub index: i64,
    /// Whether the group overrides lower-priority groups on the same range.
    #[serde(rename = "group_override")]
    pub r#override: bool,
    /// The rules in the group.
    ///
    /// Go's field is `[]*pd.Rule`, so a bundle that never had rules marshals
    /// `"rules":null` where this marshals `"rules":[]`. Nothing in TiDB reads
    /// that distinction back: PD treats both as an empty rule list.
    #[serde(rename = "rules")]
    pub rules: Vec<Rule>,
}

/// Go `NewBundle`: creates a bundle with the provided ID.
///
/// Note that you should never pass a negative id.
pub fn new_bundle(id: i64) -> Bundle {
    Bundle {
        id: group_id(id),
        ..Bundle::default()
    }
}

/// Go `NewBundleFromConstraintsOptions`: transforms constraints options into
/// the bundle.
///
/// # Errors
///
/// Returns `ErrInvalidPlacementOptions` for nil or sugar-mixed options, and
/// otherwise propagates the constraint, replica-count, and survival-preference
/// failures of the rules it builds.
pub fn new_bundle_from_constraints_options(
    options: Option<&PlacementSettings>,
) -> Result<Bundle, PlacementError> {
    let Some(options) = options else {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            "options can not be nil",
        ));
    };

    if !options.primary_region.is_empty()
        || !options.regions.is_empty()
        || !options.schedule.is_empty()
    {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            format!(
                "should be [LEADER/VOTER/LEARNER/FOLLOWER]_CONSTRAINTS=.. [VOTERS/FOLLOWERS/LEARNERS]=.., mixed other sugar options {options}"
            ),
        ));
    }

    let constraints = &options.constraints;
    let leader_const = &options.leader_constraints;
    let learner_constraints = &options.learner_constraints;
    let follower_constraints = &options.follower_constraints;
    let explicit_follower_count = options.followers;
    let explicit_learner_count = options.learners;

    let mut rules: Vec<Rule> = Vec::new();
    let common_constraints = match new_constraints_from_yaml(constraints.as_bytes()) {
        Ok(common_constraints) => common_constraints,
        Err(_) => {
            // If it's not in array format, attempt to parse it as a dictionary
            // for more detailed definitions. The dictionary format specifies
            // details for each replica. Constraints are used to define normal
            // replicas that should act as voters.
            // For example:
            // CONSTRAINTS='{ "+region=us-east-1":2, "+region=us-east-2": 2, "+region=us-west-1": 1}'
            let normal_replicas_rules = RuleBuilder::new()
                .set_role(PeerRoleType::VOTER)
                .set_constraint_str(constraints)
                .build_rules_with_dict_constraints_only()?;
            rules.extend(normal_replicas_rules);
            Vec::new()
        }
    };
    let need_create_default = rules.is_empty();
    let mut leader_constraints = new_constraints_from_yaml(leader_const.as_bytes()).map_err(
        |err| {
            err.wrapping(
                "'LeaderConstraints' should be [constraint1, ...] or any yaml compatible array representation",
            )
        },
    )?;
    for constraint in &common_constraints {
        add_constraint(&mut leader_constraints, constraint.clone())
            .map_err(|err| err.wrapping("LeaderConstraints conflicts with Constraints"))?;
    }
    let (mut leader_replicas, mut follower_replicas) = (1_u64, 2_u64);
    if explicit_follower_count > 0 {
        follower_replicas = explicit_follower_count;
    }
    if !need_create_default {
        if leader_const.is_empty() {
            leader_replicas = 0;
        }
        if follower_constraints.is_empty() {
            if explicit_follower_count > 0 {
                return Err(PlacementError::wrap(
                    PlacementErrorKind::InvalidPlacementOptions,
                    "specify follower count without specify follower constraints when specify other constraints",
                ));
            }
            follower_replicas = 0;
        }
    }

    // Create leader rule.
    // If no constraints, we need create default leader rule.
    if leader_replicas > 0 {
        let leader_rule = new_rule(PeerRoleType::LEADER, leader_replicas, leader_constraints);
        rules.push(leader_rule);
    }

    // Create follower rules.
    // If no constraints, we need create default follower rules.
    if follower_replicas > 0 {
        let mut builder = RuleBuilder::new();
        builder.set_role(PeerRoleType::VOTER);
        builder.set_replicas_num(follower_replicas);
        builder.set_skip_check_replicas_consistent(
            need_create_default && explicit_follower_count == 0,
        );
        builder.set_constraint_str(follower_constraints);
        let mut follower_rules = builder
            .build_rules()
            .map_err(|err| err.wrapping("invalid FollowerConstraints"))?;
        for follower_rule in &mut follower_rules {
            for constraint in &common_constraints {
                add_constraint(&mut follower_rule.label_constraints, constraint.clone()).map_err(
                    |err| err.wrapping("FollowerConstraints conflicts with Constraints"),
                )?;
            }
        }
        rules.extend(follower_rules);
    }

    // Create learner rules.
    let mut builder = RuleBuilder::new();
    builder.set_role(PeerRoleType::LEARNER);
    builder.set_replicas_num(explicit_learner_count);
    builder.set_constraint_str(learner_constraints);
    let mut learner_rules = builder
        .build_rules()
        .map_err(|err| err.wrapping("invalid LearnerConstraints"))?;
    for rule in &mut learner_rules {
        for constraint in &common_constraints {
            add_constraint(&mut rule.label_constraints, constraint.clone())
                .map_err(|err| err.wrapping("LearnerConstraints conflicts with Constraints"))?;
        }
    }
    rules.extend(learner_rules);
    let labels = new_location_labels_from_survival_preferences(&options.survival_preferences)?;
    for rule in &mut rules {
        rule.location_labels.clone_from(&labels);
    }
    Ok(Bundle {
        rules,
        ..Bundle::default()
    })
}

/// Go `NewBundleFromSugarOptions`: transforms syntax sugar options into the
/// bundle.
///
/// # Errors
///
/// Returns `ErrInvalidPlacementOptions` for nil options, options mixed with
/// explicit constraints, a primary region outside `REGIONS`, or an unsupported
/// `SCHEDULE`.
pub fn new_bundle_from_sugar_options(
    options: Option<&PlacementSettings>,
) -> Result<Bundle, PlacementError> {
    let Some(options) = options else {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            "options can not be nil",
        ));
    };

    if !options.leader_constraints.is_empty()
        || !options.learner_constraints.is_empty()
        || !options.follower_constraints.is_empty()
        || !options.constraints.is_empty()
        || options.learners > 0
    {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            format!(
                "should be PRIMARY_REGION=.. REGIONS=.. FOLLOWERS=.. SCHEDULE=.., mixed other constraints into options {options}"
            ),
        ));
    }

    let primary_region = go_trim_space(&options.primary_region);

    let mut regions: Vec<String> = Vec::new();
    let trimmed_regions = go_trim_space(&options.regions);
    if !trimmed_regions.is_empty() {
        regions = trimmed_regions
            .split(',')
            .map(|region| go_trim_space(region).to_owned())
            .collect();
    }

    let mut followers = options.followers;
    if followers == 0 {
        followers = 2;
    }
    let schedule = &options.schedule;

    let mut rules: Vec<Rule> = Vec::new();

    let location_labels =
        new_location_labels_from_survival_preferences(&options.survival_preferences)?;

    // In case of an empty primaryRegion and regions, just return an empty
    // bundle.
    if primary_region.is_empty() && regions.is_empty() {
        rules.push(new_rule(
            PeerRoleType::VOTER,
            followers + 1,
            new_constraints_direct(Vec::new()),
        ));
        for rule in &mut rules {
            rule.location_labels.clone_from(&location_labels);
        }
        return Ok(Bundle {
            rules,
            ..Bundle::default()
        });
    }

    // Regions must include the primary.
    regions.sort();
    let primary_index = regions.partition_point(|region| region.as_str() < primary_region);
    if primary_index >= regions.len() || regions[primary_index] != primary_region {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            "primary region must be included in regions",
        ));
    }

    // primaryCount only makes sense when len(regions) > 0, but we compute it
    // here anyway to reuse code.
    let primary_count = match schedule.go_to_lower().as_str() {
        "" | "even" => (followers + 1).div_ceil(regions.len() as u64),
        "majority_in_primary" => {
            // Calculate how many replicas need to be in the primary region for
            // quorum. Go writes `(followers+1)/2 + 1`, which for unsigned
            // arithmetic is the same value as `ceil(followers/2) + 1`.
            followers.div_ceil(2) + 1
        }
        _ => {
            return Err(PlacementError::wrap(
                PlacementErrorKind::InvalidPlacementOptions,
                format!("unsupported schedule {schedule}"),
            ));
        }
    };

    rules.push(new_rule(
        PeerRoleType::LEADER,
        1,
        new_constraints_direct(vec![new_constraint_direct(
            "region",
            LabelConstraintOp::IN,
            &[primary_region],
        )]),
    ));
    if primary_count > 1 {
        rules.push(new_rule(
            PeerRoleType::VOTER,
            primary_count - 1,
            new_constraints_direct(vec![new_constraint_direct(
                "region",
                LabelConstraintOp::IN,
                &[primary_region],
            )]),
        ));
    }
    if let Some(cnt) = (followers + 1)
        .checked_sub(primary_count)
        .filter(|cnt| *cnt > 0)
    {
        // Delete the primary from the regions.
        regions.remove(primary_index);
        if regions.is_empty() {
            rules.push(new_rule(
                PeerRoleType::VOTER,
                cnt,
                new_constraints_direct(Vec::new()),
            ));
        } else {
            let values: Vec<&str> = regions.iter().map(String::as_str).collect();
            rules.push(new_rule(
                PeerRoleType::VOTER,
                cnt,
                new_constraints_direct(vec![new_constraint_direct(
                    "region",
                    LabelConstraintOp::IN,
                    &values,
                )]),
            ));
        }
    }

    // Set location labels.
    for rule in &mut rules {
        rule.location_labels.clone_from(&location_labels);
    }

    Ok(Bundle {
        rules,
        ..Bundle::default()
    })
}

/// Go `strings.TrimSpace` over the Unicode space set Go uses.
fn go_trim_space(value: &str) -> &str {
    value.trim_matches(|character: char| character.is_whitespace())
}

/// Go `newBundleFromOptions`: non-exported functionality function, do not use
/// it directly but [`new_bundle_from_options`]; it is exposed here only because
/// Go's in-package tests call it directly.
///
/// # Errors
///
/// Returns `ErrInvalidPlacementOptions` for nil options or more than eight
/// followers, and otherwise propagates the sugar or constraints builder.
pub(crate) fn new_bundle_from_options_untidied(
    options: Option<&PlacementSettings>,
) -> Result<Bundle, PlacementError> {
    let Some(settings) = options else {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            "options can not be nil",
        ));
    };

    if settings.followers > 8 {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidPlacementOptions,
            format!(
                "followers should be less than or equal to 8: {}",
                settings.followers
            ),
        ));
    }

    // Always prefer the sugar syntax, which gives better schedule results most
    // of the time.
    let is_syntax_sugar = !(!settings.leader_constraints.is_empty()
        || !settings.learner_constraints.is_empty()
        || !settings.follower_constraints.is_empty()
        || !settings.constraints.is_empty()
        || settings.learners > 0);

    if is_syntax_sugar {
        new_bundle_from_sugar_options(options)
    } else {
        new_bundle_from_constraints_options(options)
    }
}

/// Go `newLocationLabelsFromSurvivalPreferences`: parses the survival
/// preferences into location labels.
fn new_location_labels_from_survival_preferences(
    survival_preference_str: &str,
) -> Result<Vec<String>, PlacementError> {
    if !survival_preference_str.is_empty() {
        return unmarshal_strict_string_slice(survival_preference_str.as_bytes())
            .map_err(|_| PlacementError::new(PlacementErrorKind::InvalidSurvivalPreferenceFormat));
    }
    Ok(Vec::new())
}

/// Go `NewBundleFromOptions`: transforms options into the bundle.
///
/// # Errors
///
/// Propagates [`new_bundle_from_options_untidied`] and [`Bundle::tidy`].
pub fn new_bundle_from_options(
    options: Option<&PlacementSettings>,
) -> Result<Bundle, PlacementError> {
    let mut bundle = new_bundle_from_options_untidied(options)?;
    bundle.tidy()?;
    Ok(bundle)
}

impl fmt::Display for Bundle {
    /// Go `(*Bundle).String`, which returns `""` when marshaling fails.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(test)]
        if mock_marshal_failure() {
            return Ok(());
        }
        match serde_json::to_string(self) {
            Ok(rendered) => formatter.write_str(&rendered),
            Err(_) => Ok(()),
        }
    }
}

// boundary: Go's `failpoint.Inject("MockMarshalFailure", ...)` inside
// `(*Bundle).String`. Serializing this struct cannot fail, so the injection
// point is a test-only thread-local rather than a failpoint registry.
#[cfg(test)]
thread_local! {
    static MOCK_MARSHAL_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn mock_marshal_failure() -> bool {
    MOCK_MARSHAL_FAILURE.with(std::cell::Cell::get)
}

#[cfg(test)]
fn set_mock_marshal_failure(enabled: bool) {
    MOCK_MARSHAL_FAILURE.with(|cell| cell.set(enabled));
}

/// Go `constraintsGroup`: a group of rules with the same constraints.
#[derive(Debug, Default)]
struct ConstraintsGroup {
    rules: Vec<Rule>,
    /// Whether the group has a leader/voter role; it is valid only if it has a
    /// leader.
    can_became_leader: bool,
    /// Whether a leader role is specified in this group.
    is_leader_group: bool,
}

impl ConstraintsGroup {
    /// Go `(*constraintsGroup).MergeRulesByRole`: merges the rules with the
    /// same role.
    fn merge_rules_by_role(&mut self) {
        // Create a map to store rules by role. Go's map iteration order is
        // random; keeping source order here only fixes which rule of a role
        // donates the non-count fields, and `Tidy` sorts by ID afterwards.
        let mut rules_by_role: Vec<(PeerRoleType, Vec<Rule>)> = Vec::new();

        // Iterate through each rule.
        for rule in std::mem::take(&mut self.rules) {
            let role = rule.role.clone();
            // Add the rule to the map based on its role.
            if role == PeerRoleType::LEADER || role == PeerRoleType::VOTER {
                self.can_became_leader = true;
            }
            if role == PeerRoleType::LEADER {
                self.is_leader_group = true;
            }
            match rules_by_role.iter_mut().find(|(key, _)| *key == role) {
                Some((_, rules)) => rules.push(rule),
                None => rules_by_role.push((role, vec![rule])),
            }
        }

        // Iterate through each role and merge the rules.
        for (_, rules) in rules_by_role {
            let mut rules = rules.into_iter();
            let mut merged_rule = rules.next().expect("a role bucket is never empty");
            for rule in rules {
                merged_rule.count += rule.count;
                if merged_rule.id > rule.id {
                    merged_rule.id = rule.id;
                }
            }
            self.rules.push(merged_rule);
        }
    }

    /// Go `(*constraintsGroup).MergeTransformableRoles`: merges all the rules
    /// into one that can be transformed to other roles.
    fn merge_transformable_roles(&mut self) {
        if self.rules.len() <= 1 {
            return;
        }
        let mut merged_rule: Option<Rule> = None;
        let mut new_rules = Vec::with_capacity(self.rules.len());
        for rule in std::mem::take(&mut self.rules) {
            // Learner is not transformable, it should be promoted by PD.
            if rule.role == PeerRoleType::LEARNER {
                new_rules.push(rule);
                continue;
            }
            match merged_rule.as_mut() {
                None => merged_rule = Some(rule),
                Some(merged) => {
                    merged.count += rule.count;
                    if merged.id > rule.id {
                        merged.id = rule.id;
                    }
                }
            }
        }
        if let Some(mut merged) = merged_rule {
            merged.role = PeerRoleType::VOTER;
            new_rules.push(merged);
        }
        self.rules = new_rules;
    }
}

/// Go `transformableLeaderConstraint`.
fn transformable_leader_constraint(
    groups: &mut [(String, ConstraintsGroup)],
) -> Result<(), PlacementError> {
    let mut leader_group: Option<usize> = None;
    let mut can_became_leader_num = 0;
    for (index, (_, group)) in groups.iter().enumerate() {
        if group.is_leader_group {
            if leader_group.is_some() {
                return Err(PlacementError::new(
                    PlacementErrorKind::InvalidPlacementOptions,
                ));
            }
            leader_group = Some(index);
        }
        if group.can_became_leader {
            can_became_leader_num += 1;
        }
    }
    // If there is a specified group that should have the leader, and only this
    // group can be a leader, that means the leader's priority is certain, so we
    // can merge the transformable rules into one.
    // eg:
    //  - [ group1 (L F), group2 (F) ], after merging is [group1 (2*V), group2 (F)],
    //    we still know the leader prefers group1.
    //  - [ group1 (L F), group2 (V) ], after merging is [group1 (2*V), group2 (V)],
    //    we can't know leader priority after merge.
    if let Some(index) = leader_group {
        if can_became_leader_num == 1 {
            groups[index].1.merge_transformable_roles();
        }
    }
    Ok(())
}

/// Go `hex.EncodeToString`.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Go `codec.EncodeBytes(nil, input)`.
fn encode_bytes_owned(input: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode_bytes(&mut buffer, input);
    buffer
}

/// Go `GetRangeStartAndEndKeyHex`: gets the startKeyHex and endKeyHex of the
/// range identified by `range_bundle_id`.
pub fn get_range_start_and_end_key_hex(range_bundle_id: &str) -> (String, String) {
    let (mut start_key, mut end_key) = (String::new(), String::new());
    if range_bundle_id == TIDB_BUNDLE_RANGE_PREFIX_FOR_META {
        // Use codec.EncodeBytes to properly encode the meta prefix in table mode.
        start_key = hex_encode(&encode_bytes_owned(META_PREFIX));
        end_key = hex_encode(&encode_bytes_owned(&gen_table_prefix(0)));
    }
    (start_key, end_key)
}

impl Bundle {
    /// Go `(*Bundle).Tidy`: post-optimizes the rules, trying to generate rules
    /// that suit PD.
    ///
    /// # Errors
    ///
    /// Returns `ErrInvalidPlacementOptions` when more than one constraint group
    /// claims the leader.
    pub fn tidy(&mut self) -> Result<(), PlacementError> {
        // Refer to tidb#58633.
        // Does not explicitly set an exclude rule with label.key==EngineLabelKey,
        // because PD may wrongly add a peer to unexpected stores if that key is
        // specified.
        let mut temp_rules = Vec::with_capacity(self.rules.len());
        let mut id = 0;
        for mut rule in std::mem::take(&mut self.rules) {
            // Useless rule.
            if rule.count <= 0 {
                continue;
            }
            rule.id = id.to_string();
            temp_rules.push(rule);
            id += 1;
        }

        let mut groups: Vec<(String, ConstraintsGroup)> = Vec::new();
        for rule in temp_rules {
            let key = crate::constraints::constraints_finger_print(&rule.label_constraints);
            match groups.iter_mut().find(|(existing, _)| *existing == key) {
                Some((_, group)) => group.rules.push(rule),
                None => groups.push((
                    key,
                    ConstraintsGroup {
                        rules: vec![rule],
                        ..ConstraintsGroup::default()
                    },
                )),
            }
        }
        for (_, group) in &mut groups {
            group.merge_rules_by_role();
        }
        transformable_leader_constraint(&mut groups)?;
        let mut final_rules = Vec::new();
        for (_, group) in groups {
            final_rules.extend(group.rules);
        }
        // Sort by id.
        final_rules.sort_by(|left, right| left.id.cmp(&right.id));
        self.rules = final_rules;
        Ok(())
    }

    /// Go `(*Bundle).RebuildForRange`: rebuilds the bundle for a system range.
    pub fn rebuild_for_range(&mut self, range_name: &str, policy_name: &str) -> &mut Self {
        if range_name == KEY_RANGE_GLOBAL {
            self.id = TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL.to_owned();
            self.index = RULE_INDEX_KEY_RANGE_FOR_GLOBAL;
        } else if range_name == KEY_RANGE_META {
            self.id = TIDB_BUNDLE_RANGE_PREFIX_FOR_META.to_owned();
            self.index = RULE_INDEX_KEY_RANGE_FOR_META;
        }

        let (start_key, end_key) = get_range_start_and_end_key_hex(&self.id);
        self.r#override = true;
        let mut new_rules = Vec::with_capacity(self.rules.len());
        for (index, rule) in self.rules.iter().enumerate() {
            let mut copied = rule.clone_rule();
            copied.id = format!("{}_rule_{index}", policy_name.go_to_lower());
            copied.group_id.clone_from(&self.id);
            copied.start_key_hex.clone_from(&start_key);
            copied.end_key_hex.clone_from(&end_key);
            copied.index = index as i64;
            new_rules.push(copied);
        }
        self.rules = new_rules;
        self
    }

    /// Go `(*Bundle).Reset`: resets the bundle ID and key range of all rules.
    ///
    /// # Panics
    ///
    /// Panics when `new_ids` is empty, as Go's `newIDs[0]` does.
    pub fn reset(&mut self, rule_index: i64, new_ids: &[i64]) -> &mut Self {
        // Eliminate the redundant rules.
        let mut basic_rules: Vec<Rule> = Vec::new();
        if !self.rules.is_empty() {
            // Make priority for rules with RuleIndexTable because of duplicate
            // rules existing with RuleIndexPartition. If RuleIndexTable doesn't
            // exist, the bundle itself is an independent series of rules for a
            // partition.
            for rule in &self.rules {
                if rule.index == RULE_INDEX_TABLE {
                    basic_rules.push(rule.clone());
                }
            }
            if basic_rules.is_empty() {
                basic_rules.clone_from(&self.rules);
            }
        }

        // Extend and reset the basic rules for all new ids; the first id should
        // be the group id.
        self.id = group_id(*new_ids.first().expect("Reset requires at least one id"));
        self.index = rule_index;
        self.r#override = true;
        let mut new_rules = Vec::with_capacity(basic_rules.len() * new_ids.len());
        for (index, new_id) in new_ids.iter().copied().enumerate() {
            // rule.id should be distinguished from each other, otherwise it
            // will be de-duplicated in the PD HTTP API.
            let rule_id = if rule_index == RULE_INDEX_PARTITION {
                format!("partition_rule_{new_id}")
            } else if index == 0 {
                format!("table_rule_{new_id}")
            } else {
                format!("partition_rule_{new_id}")
            };
            // Involve all the table level objects.
            let start_key = hex_encode(&encode_bytes_owned(&gen_table_prefix(new_id)));
            let end_key = hex_encode(&encode_bytes_owned(&gen_table_prefix(new_id + 1)));
            for (position, rule) in basic_rules.iter().enumerate() {
                let mut clone = rule.clone_rule();
                // For the rules of one element id, distinguish the rule ids to
                // avoid PD's overlap.
                clone.id = format!("{rule_id}_{position}");
                clone.group_id.clone_from(&self.id);
                clone.start_key_hex.clone_from(&start_key);
                clone.end_key_hex.clone_from(&end_key);
                clone.index = if index == 0 {
                    RULE_INDEX_TABLE
                } else {
                    RULE_INDEX_PARTITION
                };
                new_rules.push(clone);
            }
        }
        self.rules = new_rules;
        self
    }

    /// Go `(*Bundle).Clone`: duplicates a bundle.
    pub fn clone_bundle(&self) -> Self {
        self.clone()
    }

    /// Go `(*Bundle).IsEmpty`: checks if a bundle is empty.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty() && self.index == 0 && !self.r#override
    }

    /// Go `(*Bundle).ObjectID`: extracts the db/table/partition ID from the
    /// group ID.
    ///
    /// # Errors
    ///
    /// Returns `ErrInvalidBundleIDFormat` when the rule does not come from
    /// TiDB, and `ErrInvalidBundleID` when the suffix is not a positive
    /// integer.
    pub fn object_id(&self) -> Result<i64, PlacementError> {
        // If the rule doesn't come from TiDB, skip it.
        let Some(suffix) = self.id.strip_prefix(BUNDLE_ID_PREFIX) else {
            return Err(PlacementError::new(
                PlacementErrorKind::InvalidBundleIdFormat,
            ));
        };
        let id = suffix
            .parse::<i64>()
            .map_err(|err| PlacementError::wrap(PlacementErrorKind::InvalidBundleId, err))?;
        if id <= 0 {
            return Err(PlacementError::wrap(
                PlacementErrorKind::InvalidBundleId,
                format!("{} doesn't include an id", self.id),
            ));
        }
        Ok(id)
    }

    /// Go `(*Bundle).GetLeaderDC`: returns the leader's DC by bundle if found.
    #[must_use]
    pub fn get_leader_dc(&self, dc_label_key: &str) -> Option<String> {
        for rule in &self.rules {
            if is_valid_leader_rule(rule, dc_label_key) {
                return Some(rule.label_constraints[0].values[0].clone());
            }
        }
        None
    }
}

/// Go `isValidLeaderRule`.
fn is_valid_leader_rule(rule: &Rule, dc_label_key: &str) -> bool {
    if rule.role == PeerRoleType::LEADER && rule.count == 1 {
        for constraint in &rule.label_constraints {
            if constraint.op == LabelConstraintOp::IN
                && constraint.key == dc_label_key
                && constraint.values.len() == 1
            {
                return true;
            }
        }
    }
    false
}

/// Go `PolicyGetter`: the interface to get the policy.
pub trait PolicyGetter {
    /// Go `GetPolicy`.
    ///
    /// # Errors
    ///
    /// Returns whatever failure the underlying metadata source reports.
    fn get_policy(&self, policy_id: i64) -> Result<PolicyInfo, PlacementError>;
}

/// Go `NewTableBundle`: creates a bundle for a table key range.
///
/// If the table is a partitioned table, it also contains the rules inherited
/// from the table for every partition. The bundle does not contain the rules
/// specified independently by each partition.
///
/// # Errors
///
/// Propagates the policy lookup and [`new_bundle_from_options`].
pub fn new_table_bundle(
    getter: &impl PolicyGetter,
    table_info: &TableInfo,
) -> Result<Option<Bundle>, PlacementError> {
    let Some(mut bundle) =
        new_bundle_from_policy(getter, table_info.placement_policy_ref.as_ref())?
    else {
        return Ok(None);
    };

    let mut ids = vec![table_info.id];
    // Build the default partition rules in the table-level bundle.
    if let Some(partition) = &table_info.partition {
        for definition in partition.read().definitions.snapshot() {
            ids.push(definition.id);
        }
    }
    bundle.reset(RULE_INDEX_TABLE, &ids);
    Ok(Some(bundle))
}

/// Go `NewPartitionBundle`: creates a bundle for a partition key range.
///
/// It only contains the rules specified independently by the partition; that is
/// to say, the rules inherited from the table are not included.
///
/// # Errors
///
/// Propagates the policy lookup and [`new_bundle_from_options`].
pub fn new_partition_bundle(
    getter: &impl PolicyGetter,
    definition: &PartitionDefinition,
) -> Result<Option<Bundle>, PlacementError> {
    let bundle = new_bundle_from_policy(getter, definition.placement_policy_ref.as_ref())?;

    Ok(bundle.map(|mut bundle| {
        bundle.reset(RULE_INDEX_PARTITION, &[definition.id]);
        bundle
    }))
}

/// Go `NewPartitionListBundles`: creates a bundle list for a partition list.
///
/// # Errors
///
/// Propagates [`new_partition_bundle`].
pub fn new_partition_list_bundles(
    getter: &impl PolicyGetter,
    definitions: &[PartitionDefinition],
) -> Result<Vec<Bundle>, PlacementError> {
    let mut bundles = Vec::with_capacity(definitions.len());
    // If the partition has placement rules of its own, build the
    // partition-level bundles additionally.
    for definition in definitions {
        if let Some(bundle) = new_partition_bundle(getter, definition)? {
            bundles.push(bundle);
        }
    }
    Ok(bundles)
}

/// Go `NewFullTableBundles`: returns a bundle list with both the table bundle
/// and the partition bundles.
///
/// # Errors
///
/// Propagates [`new_table_bundle`] and [`new_partition_list_bundles`].
pub fn new_full_table_bundles(
    getter: &impl PolicyGetter,
    table_info: &TableInfo,
) -> Result<Vec<Bundle>, PlacementError> {
    let mut bundles = Vec::new();
    if let Some(table_bundle) = new_table_bundle(getter, table_info)? {
        bundles.push(table_bundle);
    }

    if let Some(partition) = &table_info.partition {
        let definitions = partition.read().definitions.snapshot();
        bundles.extend(new_partition_list_bundles(getter, &definitions)?);
    }

    Ok(bundles)
}

/// Go `newBundleFromPolicy`.
fn new_bundle_from_policy(
    getter: &impl PolicyGetter,
    policy_ref: Option<&GoShared<PolicyRefInfo>>,
) -> Result<Option<Bundle>, PlacementError> {
    let Some(policy_ref) = policy_ref else {
        return Ok(None);
    };
    let policy_id = policy_ref.read().id;
    let policy = getter.get_policy(policy_id)?;
    let settings = policy
        .placement_settings
        .as_ref()
        .map(|shared| shared.read().clone());
    new_bundle_from_options(settings.as_ref()).map(Some)
}

#[cfg(test)]
#[path = "bundle_tests.rs"]
mod tests;
