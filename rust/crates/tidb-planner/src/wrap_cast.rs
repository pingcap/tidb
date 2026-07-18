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

//! Aggregate-argument cast gating from
//! `pkg/planner/util/coreusage/cast_misc.go`.
//!
//! The Go helper intentionally owns only the mode gate: complete, first
//! partial, and deduplicate aggregates ask `AggFuncDesc.WrapCastForAggArgs`
//! to cast eligible input arguments to the aggregate return type, while final
//! and second-partial stages receive already-typed partial values. This leaf
//! models that dependency-closed mutation over opaque type-code adapters;
//! callers mark arguments that the delegated expression helper must leave
//! unchanged (for example NULL or function-specific non-cast arguments).
//! Expression construction, field type metadata, and physical projection
//! injection remain external.

use crate::aggregation_descriptor::AggFuncDesc;
pub use crate::aggregation_descriptor::AggFunctionMode;

/// Opaque aggregate argument type adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggArgument {
    type_code: u8,
    castable: bool,
}

impl AggArgument {
    /// Creates an argument with the source type-code identity.
    #[must_use]
    pub const fn new(type_code: u8) -> Self {
        Self {
            type_code,
            castable: true,
        }
    }

    /// Creates an argument that the delegated source cast helper skips.
    #[must_use]
    pub const fn uncastable(type_code: u8) -> Self {
        Self {
            type_code,
            castable: false,
        }
    }

    /// Returns the opaque type-code identity.
    #[must_use]
    pub const fn type_code(self) -> u8 {
        self.type_code
    }
}

/// Typed view of the canonical aggregate descriptor used by cast gating.
pub type AggFunction = AggFuncDesc<AggArgument, u8, ()>;

/// Creates the cast-gating view without inventing a second descriptor type.
#[must_use]
pub fn new_agg_function(
    mode: AggFunctionMode,
    return_type: u8,
    args: Vec<AggArgument>,
) -> AggFunction {
    AggFuncDesc::new("", args, return_type, mode, false, Vec::new())
}

/// Wraps aggregate arguments with the return type except for final stages.
pub fn wrap_cast_for_agg_funcs(agg_funcs: &mut [AggFunction]) {
    for agg_func in agg_funcs {
        if agg_func.mode != AggFunctionMode::Final && agg_func.mode != AggFunctionMode::Partial2 {
            for arg in &mut agg_func.args {
                if arg.castable {
                    arg.type_code = agg_func.ret_type;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_cast_modes_and_type_codes() {
        // The Go anchor builds SUM descriptors over the same five return
        // types and checks complete/final/partial behavior for both distinct
        // settings. Distinct does not participate in this helper's mode gate.
        let return_types = [1_u8, 2, 3, 4, 5];
        let modes = [
            AggFunctionMode::Complete,
            AggFunctionMode::Final,
            AggFunctionMode::Partial1,
            AggFunctionMode::Partial1,
        ];
        let mut agg_funcs = Vec::new();
        for _has_distinct in [true, false] {
            for mode in modes {
                for return_type in return_types {
                    agg_funcs.push(new_agg_function(
                        mode,
                        return_type,
                        vec![AggArgument::new(99)],
                    ));
                }
            }
        }

        let original = agg_funcs.clone();
        wrap_cast_for_agg_funcs(&mut agg_funcs);
        for (before, after) in original.iter().zip(&agg_funcs) {
            if matches!(
                after.mode(),
                AggFunctionMode::Final | AggFunctionMode::Partial2
            ) {
                assert_eq!(after.args(), before.args());
            } else {
                assert_eq!(after.args()[0].type_code(), *after.ret_type());
            }
        }
    }

    #[test]
    fn test_partial2_and_dedup_boundaries() {
        let mut agg_funcs = [
            new_agg_function(
                AggFunctionMode::Partial2,
                7,
                vec![AggArgument::new(1), AggArgument::new(2)],
            ),
            new_agg_function(AggFunctionMode::Dedup, 8, vec![AggArgument::new(3)]),
        ];
        wrap_cast_for_agg_funcs(&mut agg_funcs);
        assert_eq!(
            agg_funcs[0].args(),
            &[AggArgument::new(1), AggArgument::new(2)]
        );
        assert_eq!(agg_funcs[1].args(), &[AggArgument::new(8)]);
    }

    #[test]
    fn test_delegated_uncastable_argument_is_preserved() {
        let mut agg_funcs = [new_agg_function(
            AggFunctionMode::Complete,
            7,
            vec![AggArgument::new(1), AggArgument::uncastable(2)],
        )];
        wrap_cast_for_agg_funcs(&mut agg_funcs);
        assert_eq!(
            agg_funcs[0].args(),
            &[AggArgument::new(7), AggArgument::uncastable(2)]
        );
    }

    #[test]
    fn test_empty_argument_list_is_unchanged() {
        let mut agg_funcs = [new_agg_function(AggFunctionMode::Complete, 9, Vec::new())];
        wrap_cast_for_agg_funcs(&mut agg_funcs);
        assert!(agg_funcs[0].args().is_empty());
    }
}
