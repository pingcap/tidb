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

//! Slow-log rule metadata from `pkg/sessionctx/slowlogrule/rules.go`.
//!
//! The Go package keeps conditions grouped by AND within a rule and rules
//! grouped by OR within a scope. This leaf ports those ownership shapes plus
//! the session effective-field invalidation marker. It does not parse rule
//! text, evaluate conditions, hash raw rules, or attach rules to live session
//! variables.

use std::collections::{BTreeMap, BTreeSet};

use crate::slow_log_threshold::SlowLogValue;

/// One slow-log condition within an AND group.
#[derive(Clone, Debug, PartialEq)]
pub struct SlowLogCondition {
    /// Lowercase slow-log field name.
    pub field: String,
    /// Typed threshold value.
    pub threshold: SlowLogValue,
}

/// One slow-log rule whose conditions are evaluated together.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SlowLogRule {
    /// Conditions combined with logical AND.
    pub conditions: Vec<SlowLogCondition>,
}

/// All session-scope rules and their field index.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SlowLogRules {
    /// Original rule text before parsing.
    pub raw_rules: String,
    /// Unique fields present across all rules.
    pub fields: BTreeSet<String>,
    /// Rules combined with logical OR.
    pub rules: Vec<SlowLogRule>,
}

/// Session-effective view over session-scope rules.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionSlowLogRules {
    /// Session-scope rule set.
    pub slow_log_rules: SlowLogRules,
    /// Fields visible after combining session and global rules.
    pub effective_fields: BTreeSet<String>,
    /// Hash supplied by the global-rule owner.
    pub global_raw_rules_hash: u64,
    /// Whether effective fields need recomputation.
    pub need_update_effective_fields: bool,
}

impl SessionSlowLogRules {
    /// Creates a session view and marks effective fields stale.
    #[must_use]
    pub fn new(slow_log_rules: SlowLogRules) -> Self {
        Self {
            slow_log_rules,
            effective_fields: BTreeSet::new(),
            global_raw_rules_hash: 0,
            need_update_effective_fields: true,
        }
    }
}

/// Global slow-log rules indexed by connection ID.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GlobalSlowLogRules {
    /// Original global rule text before parsing.
    pub raw_rules: String,
    /// Hash supplied by the global-rule owner.
    pub raw_rules_hash: u64,
    /// Rules indexed by connection ID; `-1` is the global sentinel in Go.
    pub rules_map: BTreeMap<i64, SlowLogRules>,
}
