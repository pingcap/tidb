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

//! Typed payload and canonical restore for `CREATE`, `ALTER`, and `DROP
//! PLACEMENT POLICY`, owned by Go's `ddl_placement_parser.go` source domain.

use crate::util::{back_quote, escape_string_literal};

/// Placement-specific restore behavior exposed by Go's restore flags.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlacementRestoreMode {
    /// `format.DefaultRestoreFlags`.
    #[default]
    Default,
    /// `format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment`.
    SpecialComment,
    /// `format.DefaultRestoreFlags | format.SkipPlacementRuleForRestore`.
    Skip,
}

/// A `CREATE [OR REPLACE] PLACEMENT POLICY` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePlacementPolicyStmt {
    /// Whether an existing policy is atomically replaced.
    pub or_replace: bool,
    /// Whether duplicate-policy errors are suppressed.
    pub if_not_exists: bool,
    /// The policy name.
    pub name: String,
    /// Source-ordered, typed placement rules.
    pub options: Vec<PlacementOption>,
}

/// An `ALTER PLACEMENT POLICY` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPlacementPolicyStmt {
    /// Whether a missing policy is ignored.
    pub if_exists: bool,
    /// The policy name.
    pub name: String,
    /// Source-ordered replacement placement rules.
    pub options: Vec<PlacementOption>,
}

/// A `DROP PLACEMENT POLICY` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropPlacementPolicyStmt {
    /// Whether a missing policy is ignored.
    pub if_exists: bool,
    /// The policy name.
    pub name: String,
}

/// One typed placement-policy option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementOption {
    /// `PRIMARY_REGION = 'region'`.
    PrimaryRegion(String),
    /// `REGIONS = 'region,...'`.
    Regions(String),
    /// `FOLLOWERS = n`.
    Followers(u64),
    /// `VOTERS = n`.
    Voters(u64),
    /// `LEARNERS = n`.
    Learners(u64),
    /// `SCHEDULE = 'even'`.
    Schedule(String),
    /// `CONSTRAINTS = '...'`.
    Constraints(String),
    /// `LEADER_CONSTRAINTS = '...'`.
    LeaderConstraints(String),
    /// `FOLLOWER_CONSTRAINTS = '...'`.
    FollowerConstraints(String),
    /// `VOTER_CONSTRAINTS = '...'`.
    VoterConstraints(String),
    /// `LEARNER_CONSTRAINTS = '...'`.
    LearnerConstraints(String),
    /// `SURVIVAL_PREFERENCES = '...'`.
    SurvivalPreferences(String),
    /// `PLACEMENT POLICY = name`, used by `ALTER RANGE` and table options.
    Policy(String),
}

impl PlacementOption {
    pub(crate) fn restore_into(&self, out: &mut String, mode: PlacementRestoreMode) {
        if mode == PlacementRestoreMode::Skip {
            return;
        }
        let (name, value) = match self {
            Self::PrimaryRegion(value) => ("PRIMARY_REGION", Some(value)),
            Self::Regions(value) => ("REGIONS", Some(value)),
            Self::Schedule(value) => ("SCHEDULE", Some(value)),
            Self::Constraints(value) => ("CONSTRAINTS", Some(value)),
            Self::LeaderConstraints(value) => ("LEADER_CONSTRAINTS", Some(value)),
            Self::FollowerConstraints(value) => ("FOLLOWER_CONSTRAINTS", Some(value)),
            Self::VoterConstraints(value) => ("VOTER_CONSTRAINTS", Some(value)),
            Self::LearnerConstraints(value) => ("LEARNER_CONSTRAINTS", Some(value)),
            Self::SurvivalPreferences(value) => ("SURVIVAL_PREFERENCES", Some(value)),
            Self::Policy(value) => {
                out.push_str("PLACEMENT POLICY = ");
                out.push_str(&back_quote(value));
                return;
            }
            Self::Followers(value) => {
                out.push_str("FOLLOWERS = ");
                out.push_str(&value.to_string());
                return;
            }
            Self::Voters(value) => {
                out.push_str("VOTERS = ");
                out.push_str(&value.to_string());
                return;
            }
            Self::Learners(value) => {
                out.push_str("LEARNERS = ");
                out.push_str(&value.to_string());
                return;
            }
        };
        out.push_str(name);
        out.push_str(" = '");
        out.push_str(&escape_string_literal(
            value.expect("string placement option"),
        ));
        out.push('\'');
    }
}

impl CreatePlacementPolicyStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_mode_into(out, PlacementRestoreMode::Default);
    }

    /// Restores with the placement-specific Go flag behavior.
    pub fn restore_with_mode(&self, mode: PlacementRestoreMode) -> String {
        let mut out = String::new();
        self.restore_mode_into(&mut out, mode);
        out
    }

    fn restore_mode_into(&self, out: &mut String, mode: PlacementRestoreMode) {
        if mode == PlacementRestoreMode::SpecialComment {
            out.push_str("/*T![placement] ");
            self.restore_mode_into(out, PlacementRestoreMode::Default);
            out.push_str(" */");
            return;
        }
        out.push_str("CREATE ");
        if self.or_replace {
            out.push_str("OR REPLACE ");
        }
        out.push_str("PLACEMENT POLICY ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        for option in &self.options {
            out.push(' ');
            option.restore_into(out, mode);
        }
    }
}

impl AlterPlacementPolicyStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_mode_into(out, PlacementRestoreMode::Default);
    }

    /// Restores with the placement-specific Go flag behavior.
    pub fn restore_with_mode(&self, mode: PlacementRestoreMode) -> String {
        let mut out = String::new();
        self.restore_mode_into(&mut out, mode);
        out
    }

    fn restore_mode_into(&self, out: &mut String, mode: PlacementRestoreMode) {
        if mode == PlacementRestoreMode::Skip {
            return;
        }
        if mode == PlacementRestoreMode::SpecialComment {
            out.push_str("/*T![placement] ");
            self.restore_mode_into(out, PlacementRestoreMode::Default);
            out.push_str(" */");
            return;
        }
        out.push_str("ALTER PLACEMENT POLICY ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        for option in &self.options {
            out.push(' ');
            option.restore_into(out, mode);
        }
    }
}

impl DropPlacementPolicyStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_mode_into(out, PlacementRestoreMode::Default);
    }

    /// Restores with the placement-specific Go flag behavior.
    pub fn restore_with_mode(&self, mode: PlacementRestoreMode) -> String {
        let mut out = String::new();
        self.restore_mode_into(&mut out, mode);
        out
    }

    fn restore_mode_into(&self, out: &mut String, mode: PlacementRestoreMode) {
        if mode == PlacementRestoreMode::SpecialComment {
            out.push_str("/*T![placement] ");
            self.restore_mode_into(out, PlacementRestoreMode::Default);
            out.push_str(" */");
            return;
        }
        out.push_str("DROP PLACEMENT POLICY ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for PlacementRestoreMode {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::SpecialComment => {}
            Self::Skip => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreatePlacementPolicyStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            or_replace,
            if_not_exists,
            name,
            options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = or_replace;
        let _ = if_not_exists;
        let _ = name;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterPlacementPolicyStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_exists,
            name,
            options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = if_exists;
        let _ = name;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropPlacementPolicyStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { if_exists, name } = self;
        let _ = if_exists;
        let _ = name;
        visitor.leave(self)
    }
}

impl crate::Visitable for PlacementOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::PrimaryRegion(field_0) => {
                let _ = field_0;
            }
            Self::Regions(field_0) => {
                let _ = field_0;
            }
            Self::Followers(field_0) => {
                let _ = field_0;
            }
            Self::Voters(field_0) => {
                let _ = field_0;
            }
            Self::Learners(field_0) => {
                let _ = field_0;
            }
            Self::Schedule(field_0) => {
                let _ = field_0;
            }
            Self::Constraints(field_0) => {
                let _ = field_0;
            }
            Self::LeaderConstraints(field_0) => {
                let _ = field_0;
            }
            Self::FollowerConstraints(field_0) => {
                let _ = field_0;
            }
            Self::VoterConstraints(field_0) => {
                let _ = field_0;
            }
            Self::LearnerConstraints(field_0) => {
                let _ = field_0;
            }
            Self::SurvivalPreferences(field_0) => {
                let _ = field_0;
            }
            Self::Policy(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
