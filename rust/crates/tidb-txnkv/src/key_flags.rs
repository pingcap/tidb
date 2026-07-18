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

//! Per-key metadata translated from `pkg/kv/keyflags.go`.
//!
//! Go stores six flags in one byte, including two bits whose four combinations
//! encode the assertion state. Rust represents the same 64 semantic states as
//! four booleans and one closed enum. This removes unused-bit and invalid-state
//! construction while preserving every operation exposed to TiDB consumers.

/// The four assertion states encoded by Go's two assertion flag bits.
#[derive(Debug, Clone, Copy, Default, Eq, Hash, PartialEq)]
pub enum AssertionState {
    /// No assertion has been set yet.
    #[default]
    Unset,
    /// The key is asserted to exist before the current transaction.
    Exists,
    /// The key is asserted not to exist before the current transaction.
    NotExists,
    /// No existence assertion can be made for the key.
    Unknown,
}

/// A non-assertion operation accepted by Go `ApplyFlagsOps`.
///
/// Assertion operations deliberately use a different type so callers cannot
/// accidentally pass assertion state through the general flag path.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum FlagsOp {
    /// Mark the key for a lazy existence check.
    SetPresumeKeyNotExists,
    /// Mark the key as requiring a pessimistic lock.
    SetNeedLocked,
    /// Postpone conflict and constraint checking until prewrite or a later lock.
    SetNeedConstraintCheckInPrewrite,
    /// Record that presume-key-not-exists was set by a previous statement.
    SetPreviousPresumeKeyNotExists,
}

/// Semantic metadata associated with one transaction-buffer key.
///
/// All fields are monotonic under [`KeyFlags::apply_flags_ops`], matching the
/// bitwise-OR behavior in Go. Assertion replacement is implemented separately
/// in `assertion.rs` because Go intentionally gives assertion operations their
/// own type.
#[derive(Debug, Clone, Copy, Default, Eq, Hash, PartialEq)]
pub struct KeyFlags {
    presume_key_not_exists: bool,
    need_locked: bool,
    assertion: AssertionState,
    need_constraint_check_in_prewrite: bool,
    previous_presume_key_not_exists: bool,
}

impl KeyFlags {
    /// Creates flags with no metadata set.
    pub const fn new() -> Self {
        Self {
            presume_key_not_exists: false,
            need_locked: false,
            assertion: AssertionState::Unset,
            need_constraint_check_in_prewrite: false,
            previous_presume_key_not_exists: false,
        }
    }

    /// Returns whether the key uses a lazy existence check.
    pub const fn has_presume_key_not_exists(self) -> bool {
        self.presume_key_not_exists
    }

    /// Returns whether the key must be pessimistically locked.
    pub const fn has_need_locked(self) -> bool {
        self.need_locked
    }

    /// Returns the key's normalized assertion state.
    pub const fn assertion(self) -> AssertionState {
        self.assertion
    }

    /// Returns whether the key is asserted to exist.
    pub const fn has_assert_exists(self) -> bool {
        matches!(self.assertion, AssertionState::Exists)
    }

    /// Returns whether the key is asserted not to exist.
    pub const fn has_assert_not_exists(self) -> bool {
        matches!(self.assertion, AssertionState::NotExists)
    }

    /// Returns whether the key cannot carry an existence assertion.
    pub const fn has_assert_unknown(self) -> bool {
        matches!(self.assertion, AssertionState::Unknown)
    }

    /// Returns whether any assertion state has been set for the key.
    pub const fn has_assertion_flags(self) -> bool {
        !matches!(self.assertion, AssertionState::Unset)
    }

    /// Returns whether constraint and conflict checks are postponed to prewrite.
    pub const fn has_need_constraint_check_in_prewrite(self) -> bool {
        self.need_constraint_check_in_prewrite
    }

    /// Applies the supplied non-assertion operations in source order.
    ///
    /// Repeating or reordering operations has no effect because every operation
    /// only sets its associated flag, exactly like Go's bitwise OR.
    #[must_use]
    pub fn apply_flags_ops(mut self, ops: impl IntoIterator<Item = FlagsOp>) -> Self {
        for op in ops {
            match op {
                FlagsOp::SetPresumeKeyNotExists => self.presume_key_not_exists = true,
                FlagsOp::SetNeedLocked => self.need_locked = true,
                FlagsOp::SetNeedConstraintCheckInPrewrite => {
                    self.need_constraint_check_in_prewrite = true;
                }
                FlagsOp::SetPreviousPresumeKeyNotExists => {
                    self.previous_presume_key_not_exists = true;
                }
            }
        }
        self
    }

    pub(crate) const fn with_assertion_state(mut self, assertion: AssertionState) -> Self {
        self.assertion = assertion;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{AssertionState, FlagsOp, KeyFlags};
    use std::collections::HashSet;

    const ASSERTION_STATES: [AssertionState; 4] = [
        AssertionState::Unset,
        AssertionState::Exists,
        AssertionState::NotExists,
        AssertionState::Unknown,
    ];
    const FLAGS_OPS: [FlagsOp; 4] = [
        FlagsOp::SetPresumeKeyNotExists,
        FlagsOp::SetNeedLocked,
        FlagsOp::SetNeedConstraintCheckInPrewrite,
        FlagsOp::SetPreviousPresumeKeyNotExists,
    ];

    fn source_bits(flags: KeyFlags) -> u8 {
        u8::from(flags.presume_key_not_exists)
            | (u8::from(flags.need_locked) << 1)
            | (match flags.assertion {
                AssertionState::Unset => 0,
                AssertionState::Exists => 1,
                AssertionState::NotExists => 2,
                AssertionState::Unknown => 3,
            } << 2)
            | (u8::from(flags.need_constraint_check_in_prewrite) << 4)
            | (u8::from(flags.previous_presume_key_not_exists) << 5)
    }

    fn all_semantic_states() -> Vec<KeyFlags> {
        let mut states = Vec::with_capacity(64);
        for presume_key_not_exists in [false, true] {
            for need_locked in [false, true] {
                for assertion in ASSERTION_STATES {
                    for need_constraint_check_in_prewrite in [false, true] {
                        for previous_presume_key_not_exists in [false, true] {
                            states.push(KeyFlags {
                                presume_key_not_exists,
                                need_locked,
                                assertion,
                                need_constraint_check_in_prewrite,
                                previous_presume_key_not_exists,
                            });
                        }
                    }
                }
            }
        }
        states
    }

    #[test]
    fn all_64_source_states_have_exact_queries() {
        let states = all_semantic_states();
        assert_eq!(states.len(), 64);
        assert_eq!(states.iter().copied().collect::<HashSet<_>>().len(), 64);
        assert_eq!(
            states
                .iter()
                .copied()
                .map(source_bits)
                .collect::<HashSet<_>>(),
            (0_u8..64).collect()
        );

        for flags in states {
            let bits = source_bits(flags);
            assert_eq!(flags.has_presume_key_not_exists(), bits & 1 != 0);
            assert_eq!(flags.has_need_locked(), bits & 2 != 0);
            assert_eq!(flags.assertion(), flags.assertion);
            assert_eq!(flags.has_assert_exists(), bits & 0b1100 == 0b0100);
            assert_eq!(flags.has_assert_not_exists(), bits & 0b1100 == 0b1000);
            assert_eq!(flags.has_assert_unknown(), bits & 0b1100 == 0b1100);
            assert_eq!(flags.has_assertion_flags(), bits & 0b1100 != 0);
            assert_eq!(
                flags.has_need_constraint_check_in_prewrite(),
                bits & 0b1_0000 != 0
            );
        }
    }

    #[test]
    fn every_flags_operation_sets_only_its_source_state() {
        for origin in all_semantic_states() {
            for op in FLAGS_OPS {
                let actual = origin.apply_flags_ops([op]);
                let mut expected = origin;
                match op {
                    FlagsOp::SetPresumeKeyNotExists => {
                        expected.presume_key_not_exists = true;
                    }
                    FlagsOp::SetNeedLocked => expected.need_locked = true,
                    FlagsOp::SetNeedConstraintCheckInPrewrite => {
                        expected.need_constraint_check_in_prewrite = true;
                    }
                    FlagsOp::SetPreviousPresumeKeyNotExists => {
                        expected.previous_presume_key_not_exists = true;
                    }
                }
                assert_eq!(actual, expected, "origin={origin:?}, op={op:?}");
                let op_bit = match op {
                    FlagsOp::SetPresumeKeyNotExists => 1,
                    FlagsOp::SetNeedLocked => 2,
                    FlagsOp::SetNeedConstraintCheckInPrewrite => 0b1_0000,
                    FlagsOp::SetPreviousPresumeKeyNotExists => 0b10_0000,
                };
                assert_eq!(source_bits(actual), source_bits(origin) | op_bit);
            }
        }
    }

    #[test]
    fn flags_operations_preserve_empty_repeated_and_variadic_semantics() {
        let origin = KeyFlags::new().with_assertion_state(AssertionState::NotExists);
        assert_eq!(KeyFlags::default(), KeyFlags::new());
        assert_eq!(origin.apply_flags_ops([]), origin);

        let once = origin.apply_flags_ops(FLAGS_OPS);
        let repeated = origin.apply_flags_ops(FLAGS_OPS.into_iter().cycle().take(12));
        let reversed = origin.apply_flags_ops(FLAGS_OPS.into_iter().rev());
        assert_eq!(once, repeated);
        assert_eq!(once, reversed);
        assert_eq!(once.assertion(), AssertionState::NotExists);
    }
}
