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

//! Canonical aggregate descriptor metadata from
//! `pkg/expression/aggregation/{aggregation,descriptor}.go`.
//!
//! The Go descriptor is the shared planner/executor contract: name, ordered
//! arguments, return type, execution mode, DISTINCT, and aggregate-local
//! ORDER BY. Rust previously split those fields between logical-plan identity,
//! cast gating, runtime string matches, and result typing. This module keeps
//! one generic descriptor shape and one aggregate-name/mode authority while
//! allowing each consumer to retain its own typed argument/return adapters.

/// Aggregate execution stage from Go `AggFunctionMode`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum AggFunctionMode {
    /// Origin rows produce the final result.
    #[default]
    Complete,
    /// Partial rows produce the final result.
    Final,
    /// Origin rows produce partial state.
    Partial1,
    /// Partial rows produce partial state.
    Partial2,
    /// Origin rows are deduplicated without changing their shape.
    Dedup,
}

impl AggFunctionMode {
    /// Returns Go `AggFunctionMode.ToString`'s spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Final => "final",
            Self::Partial1 => "partial1",
            Self::Partial2 => "partial2",
            Self::Dedup => "deduplicate",
        }
    }

    /// Returns the source enum ordinal used by Hash64/protobuf metadata.
    #[must_use]
    pub const fn ordinal(self) -> i64 {
        match self {
            Self::Complete => 0,
            Self::Final => 1,
            Self::Partial1 => 2,
            Self::Partial2 => 3,
            Self::Dedup => 4,
        }
    }
}

/// Union of aggregate function names used by Go `NewDistAggFunc` and the
/// metadata helpers in `aggregation.go`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AggregateKind {
    /// SUM.
    Sum,
    /// Integer-specialized SUM used by coprocessors.
    SumInt,
    /// COUNT.
    Count,
    /// AVG.
    Avg,
    /// GROUP_CONCAT.
    GroupConcat,
    /// MAX.
    Max,
    /// MIN.
    Min,
    /// FIRST_ROW.
    FirstRow,
    /// VAR_POP (including the SQL `VARIANCE` alias after parser normalization).
    VarPop,
    /// VAR_SAMP.
    VarSamp,
    /// STDDEV_POP (including the SQL `STD` and `STDDEV` aliases).
    StddevPop,
    /// STDDEV_SAMP.
    StddevSamp,
    /// BIT_OR.
    BitOr,
    /// BIT_XOR.
    BitXor,
    /// BIT_AND.
    BitAnd,
    /// APPROX_COUNT_DISTINCT.
    ApproxCountDistinct,
    /// APPROX_PERCENTILE.
    ApproxPercentile,
}

impl AggregateKind {
    /// Resolves the case-insensitive SQL/AST name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("sum") {
            Some(Self::Sum)
        } else if name.eq_ignore_ascii_case("sum_int") {
            Some(Self::SumInt)
        } else if name.eq_ignore_ascii_case("count") {
            Some(Self::Count)
        } else if name.eq_ignore_ascii_case("avg") {
            Some(Self::Avg)
        } else if name.eq_ignore_ascii_case("group_concat") {
            Some(Self::GroupConcat)
        } else if name.eq_ignore_ascii_case("max") {
            Some(Self::Max)
        } else if name.eq_ignore_ascii_case("min") {
            Some(Self::Min)
        } else if name.eq_ignore_ascii_case("firstrow") {
            Some(Self::FirstRow)
        } else if name.eq_ignore_ascii_case("var_pop") || name.eq_ignore_ascii_case("variance") {
            Some(Self::VarPop)
        } else if name.eq_ignore_ascii_case("var_samp") {
            Some(Self::VarSamp)
        } else if name.eq_ignore_ascii_case("stddev_pop")
            || name.eq_ignore_ascii_case("stddev")
            || name.eq_ignore_ascii_case("std")
        {
            Some(Self::StddevPop)
        } else if name.eq_ignore_ascii_case("stddev_samp") {
            Some(Self::StddevSamp)
        } else if name.eq_ignore_ascii_case("bit_or") {
            Some(Self::BitOr)
        } else if name.eq_ignore_ascii_case("bit_xor") {
            Some(Self::BitXor)
        } else if name.eq_ignore_ascii_case("bit_and") {
            Some(Self::BitAnd)
        } else if name.eq_ignore_ascii_case("approx_count_distinct") {
            Some(Self::ApproxCountDistinct)
        } else if name.eq_ignore_ascii_case("approx_percentile") {
            Some(Self::ApproxPercentile)
        } else {
            None
        }
    }

    /// Whether Go aggregate evaluation records a count partial result.
    #[must_use]
    pub const fn needs_count(self) -> bool {
        matches!(self, Self::Count | Self::Avg)
    }

    /// Whether Go aggregate evaluation records a value partial result.
    #[must_use]
    pub const fn needs_value(self) -> bool {
        !matches!(
            self,
            Self::Count
                | Self::VarPop
                | Self::VarSamp
                | Self::StddevPop
                | Self::StddevSamp
                | Self::ApproxCountDistinct
        )
    }
}

/// The shared source-shaped aggregate descriptor. Generic fields prevent a
/// dependency inversion: planner identity, cast gating, and executor AST each
/// supply their existing typed adapters without inventing another descriptor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggFuncDesc<A, R, O> {
    pub(crate) name: String,
    pub(crate) args: Vec<A>,
    pub(crate) ret_type: R,
    pub(crate) mode: AggFunctionMode,
    pub(crate) has_distinct: bool,
    pub(crate) order_by: Vec<O>,
}

impl<A, R, O> AggFuncDesc<A, R, O> {
    /// Constructs a descriptor and canonicalizes the function name like
    /// Go `newBaseFuncDesc`.
    #[must_use]
    pub fn new(
        name: impl AsRef<str>,
        args: Vec<A>,
        ret_type: R,
        mode: AggFunctionMode,
        has_distinct: bool,
        order_by: Vec<O>,
    ) -> Self {
        Self {
            name: name.as_ref().to_ascii_lowercase(),
            args,
            ret_type,
            mode,
            has_distinct,
            order_by,
        }
    }

    #[must_use]
    /// Returns the canonical lower-case aggregate name.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    /// Returns the ordered aggregate arguments.
    pub fn args(&self) -> &[A] {
        &self.args
    }

    #[must_use]
    /// Returns the inferred aggregate result type adapter.
    pub const fn ret_type(&self) -> &R {
        &self.ret_type
    }

    #[must_use]
    /// Returns the aggregate execution stage.
    pub const fn mode(&self) -> AggFunctionMode {
        self.mode
    }

    #[must_use]
    /// Returns whether DISTINCT is applied before folding.
    pub const fn has_distinct(&self) -> bool {
        self.has_distinct
    }

    #[must_use]
    /// Returns aggregate-local ORDER BY items.
    pub fn order_by(&self) -> &[O] {
        &self.order_by
    }
}

#[cfg(test)]
mod tests {
    use super::{AggFunctionMode, AggregateKind};

    #[test]
    fn mode_and_partial_state_helpers_match_source() {
        assert_eq!(AggFunctionMode::Complete.as_str(), "complete");
        assert_eq!(AggFunctionMode::Final.as_str(), "final");
        assert_eq!(AggFunctionMode::Partial1.as_str(), "partial1");
        assert_eq!(AggFunctionMode::Partial2.as_str(), "partial2");
        assert_eq!(AggFunctionMode::Dedup.as_str(), "deduplicate");
        let cases = [
            ("sum", AggregateKind::Sum, false, true),
            ("sum_int", AggregateKind::SumInt, false, true),
            ("count", AggregateKind::Count, true, false),
            ("avg", AggregateKind::Avg, true, true),
            ("group_concat", AggregateKind::GroupConcat, false, true),
            ("max", AggregateKind::Max, false, true),
            ("min", AggregateKind::Min, false, true),
            ("firstrow", AggregateKind::FirstRow, false, true),
            ("var_pop", AggregateKind::VarPop, false, false),
            ("var_samp", AggregateKind::VarSamp, false, false),
            ("stddev_pop", AggregateKind::StddevPop, false, false),
            ("stddev_samp", AggregateKind::StddevSamp, false, false),
            ("bit_or", AggregateKind::BitOr, false, true),
            ("bit_xor", AggregateKind::BitXor, false, true),
            ("bit_and", AggregateKind::BitAnd, false, true),
            (
                "approx_count_distinct",
                AggregateKind::ApproxCountDistinct,
                false,
                false,
            ),
            (
                "approx_percentile",
                AggregateKind::ApproxPercentile,
                false,
                true,
            ),
        ];
        for (name, kind, needs_count, needs_value) in cases {
            assert_eq!(AggregateKind::from_name(name), Some(kind));
            assert_eq!(
                AggregateKind::from_name(&name.to_ascii_uppercase()),
                Some(kind)
            );
            assert_eq!(kind.needs_count(), needs_count);
            assert_eq!(kind.needs_value(), needs_value);
        }
        assert_eq!(
            AggregateKind::from_name("variance"),
            Some(AggregateKind::VarPop)
        );
        assert_eq!(
            AggregateKind::from_name("std"),
            Some(AggregateKind::StddevPop)
        );
        assert_eq!(
            AggregateKind::from_name("stddev"),
            Some(AggregateKind::StddevPop)
        );
        assert_eq!(AggregateKind::from_name("whatever"), None);
    }
}
