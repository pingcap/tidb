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

//! `CREATE`, `ALTER` and `DROP PLACEMENT POLICY`.
//!
//! A placement policy names where a schema object's replicas should live. It
//! is a schema object in its own right: tables and partitions REFERENCE it
//! rather than embedding a copy, so altering one policy changes what every
//! referencing object means, and dropping one has to be refused while
//! anything still points at it.
//!
//! Go's statement layer is `executor.CreatePlacementPolicy`,
//! `AlterPlacementPolicy` and `DropPlacementPolicy`
//! (`pkg/ddl/executor.go:6802` onward), over the settings builder
//! `buildPolicyInfo` / `SetDirectPlacementOpt`
//! (`pkg/ddl/placement_policy.go:509`).

use tidb_ast::{
    AlterPlacementPolicyStmt, CreatePlacementPolicyStmt, DropPlacementPolicyStmt, PlacementOption,
};
use tidb_model::{PlacementSettings, PolicyInfo};

use super::{Catalog, DriverError};

/// Go `SetDirectPlacementOpt` (`ddl/placement_policy.go:530`): folds the
/// source-ordered options into one settings record.
///
/// Go applies them in written order and lets a later option overwrite an
/// earlier one of the same kind, which is what makes
/// `FOLLOWERS=2 FOLLOWERS=3` mean three.
fn settings_from_options(options: &[PlacementOption]) -> Result<PlacementSettings, DriverError> {
    let mut settings = PlacementSettings::default();
    for option in options {
        match option {
            PlacementOption::PrimaryRegion(value) => settings.primary_region = value.clone(),
            PlacementOption::Regions(value) => settings.regions = value.clone(),
            PlacementOption::Followers(value) => settings.followers = *value,
            PlacementOption::Voters(value) => settings.voters = *value,
            PlacementOption::Learners(value) => settings.learners = *value,
            PlacementOption::Schedule(value) => settings.schedule = value.clone(),
            PlacementOption::Constraints(value) => settings.constraints = value.clone(),
            PlacementOption::LeaderConstraints(value) => {
                settings.leader_constraints = value.clone();
            }
            PlacementOption::FollowerConstraints(value) => {
                settings.follower_constraints = value.clone();
            }
            PlacementOption::VoterConstraints(value) => settings.voter_constraints = value.clone(),
            PlacementOption::LearnerConstraints(value) => {
                settings.learner_constraints = value.clone();
            }
            PlacementOption::SurvivalPreferences(value) => {
                settings.survival_preferences = value.clone();
            }
            // Go's `SetDirectPlacementOpt` has no arm for `PLACEMENT POLICY =
            // name` -- that spelling belongs to a table option or `ALTER
            // RANGE`, not to a policy's own definition -- and its `default`
            // arm refuses what it does not know.
            PlacementOption::Policy(_) => {
                return Err(DriverError::unsupported(
                    "PLACEMENT POLICY = <name> is not a setting of a policy itself".to_owned(),
                ));
            }
        }
    }
    Ok(settings)
}

/// Go `executor.CreatePlacementPolicy` (`ddl/executor.go:6802`).
pub fn run_create_placement_policy(
    catalog: &mut Catalog,
    statement: &CreatePlacementPolicyStmt,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // Go checks this pairing BEFORE building the settings, so a statement
    // that is both contradictory and malformed reports the contradiction.
    if statement.or_replace && statement.if_not_exists {
        return Err(DriverError::WrongUsage {
            first: "OR REPLACE",
            second: "IF NOT EXISTS",
        });
    }
    let settings = settings_from_options(&statement.options)?;
    let policy = PolicyInfo {
        placement_settings: Some(tidb_model::GoShared::new(settings.clone())),
        id: 0,
        name: tidb_ast::CiString::new(statement.name.clone()),
        state: tidb_model::SchemaState::PUBLIC,
    };
    if catalog.create_policy(policy) {
        return Ok(());
    }
    // Go's `OnExist` fork, reached only when one of that name already exists.
    if statement.if_not_exists {
        ctx.append_suppressed(&DriverError::PlacementPolicyExists(statement.name.clone()));
        return Ok(());
    }
    if statement.or_replace {
        // Go's `OnExistReplace` keeps the policy OBJECT and swaps its
        // settings, so every reference by id stays pointed at the right
        // thing.
        catalog.replace_policy_settings(&statement.name, settings);
        return Ok(());
    }
    Err(DriverError::PlacementPolicyExists(statement.name.clone()))
}

/// Go `executor.AlterPlacementPolicy` (`ddl/executor.go`).
pub fn run_alter_placement_policy(
    catalog: &mut Catalog,
    statement: &AlterPlacementPolicyStmt,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let settings = settings_from_options(&statement.options)?;
    if catalog.replace_policy_settings(&statement.name, settings) {
        return Ok(());
    }
    if statement.if_exists {
        ctx.append_suppressed(&DriverError::PlacementPolicyNotExists(
            statement.name.clone(),
        ));
        return Ok(());
    }
    Err(DriverError::PlacementPolicyNotExists(statement.name.clone()))
}

/// Go `executor.DropPlacementPolicy` (`ddl/executor.go:6829`).
pub fn run_drop_placement_policy(
    catalog: &mut Catalog,
    statement: &DropPlacementPolicyStmt,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // Go looks the policy up FIRST and only then checks whether it is in
    // use, so `IF EXISTS` over a missing policy is a note rather than an
    // in-use error.
    if catalog.policy(&statement.name).is_none() {
        if statement.if_exists {
            ctx.append_suppressed(&DriverError::PlacementPolicyNotExists(
                statement.name.clone(),
            ));
            return Ok(());
        }
        return Err(DriverError::PlacementPolicyNotExists(statement.name.clone()));
    }
    // Go `CheckPlacementPolicyNotInUseFromInfoSchema`: dropping a policy that
    // something still names would leave that object pointing at nothing.
    // `IF EXISTS` does NOT suppress this -- the policy DOES exist.
    if catalog.policy_in_use(&statement.name) {
        return Err(DriverError::PlacementPolicyInUse(statement.name.clone()));
    }
    catalog.drop_policy(&statement.name);
    Ok(())
}
