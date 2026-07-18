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

//! Cascades rule kinds from `pkg/planner/cascades/rule/rule_type.go`.
//!
//! The Go source uses an `iota`-backed integer and intentionally exposes only
//! one diagnostic label today: `XFJoinToApply` renders as `join_to_apply`,
//! while every other value renders as `default_none`. The raw integer and the
//! source fallback are retained so future rule additions do not collapse into
//! an existing variant.

use std::fmt;

/// Rule kind used by the cascades transformation registry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuleType {
    /// No rule.
    DefaultNone,
    /// Convert a join into an apply.
    XfJoinToApply,
    /// Decorrelate an apply without inner correlated predicates.
    XfDeCorrelateSimpleApply,
    /// Pull a correlated predicate through a projection.
    XfPullCorrPredFromProj,
    /// Pull a correlated predicate through a selection.
    XfPullCorrPredFromSel,
    /// Pull a correlated predicate through a data source.
    XfPullCorrPredFromDs,
    /// Pull a correlated predicate through a sort.
    XfPullCorrPredFromSort,
    /// Pull a correlated predicate through a limit.
    XfPullCorrPredFromLimit,
    /// Pull a correlated predicate through a max-one-row operator.
    XfPullCorrPredFromMax1Row,
    /// Pull a correlated predicate through the first aggregation shape.
    XfPullCorrPredFromAgg1,
    /// Pull a correlated predicate through the second aggregation shape.
    XfPullCorrPredFromAgg2,
    /// Sentinel one past the final rule kind.
    XfMaximumRuleLength,
    /// An integer not present in the source `iota` sequence.
    Unknown(i32),
}

impl RuleType {
    /// Converts the source integer representation into a typed rule kind.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::DefaultNone,
            1 => Self::XfJoinToApply,
            2 => Self::XfDeCorrelateSimpleApply,
            3 => Self::XfPullCorrPredFromProj,
            4 => Self::XfPullCorrPredFromSel,
            5 => Self::XfPullCorrPredFromDs,
            6 => Self::XfPullCorrPredFromSort,
            7 => Self::XfPullCorrPredFromLimit,
            8 => Self::XfPullCorrPredFromMax1Row,
            9 => Self::XfPullCorrPredFromAgg1,
            10 => Self::XfPullCorrPredFromAgg2,
            11 => Self::XfMaximumRuleLength,
            other => Self::Unknown(other),
        }
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> i32 {
        match self {
            Self::DefaultNone => 0,
            Self::XfJoinToApply => 1,
            Self::XfDeCorrelateSimpleApply => 2,
            Self::XfPullCorrPredFromProj => 3,
            Self::XfPullCorrPredFromSel => 4,
            Self::XfPullCorrPredFromDs => 5,
            Self::XfPullCorrPredFromSort => 6,
            Self::XfPullCorrPredFromLimit => 7,
            Self::XfPullCorrPredFromMax1Row => 8,
            Self::XfPullCorrPredFromAgg1 => 9,
            Self::XfPullCorrPredFromAgg2 => 10,
            Self::XfMaximumRuleLength => 11,
            Self::Unknown(raw) => raw,
        }
    }

    /// Returns the source `String` implementation's diagnostic label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::XfJoinToApply => "join_to_apply",
            _ => "default_none",
        }
    }
}

impl fmt::Display for RuleType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
