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

//! Scalar `GROUPING` metadata and grouping-id evaluation.
//!
//! TiDB rewrites a user-facing `GROUPING(...)` expression into a grouping-id
//! column plus this metadata before execution.  This leaf deliberately owns
//! only the pure bit calculation: the planner rewrite, tipb metadata, session
//! context, and vectorized chunk path remain outside the seed evaluator.

use std::collections::BTreeSet;

/// The three grouping-id comparison algorithms used by TiDB's tipb payload.
///
/// The discriminants match `tipb.GroupingMode` (`1`, `2`, and `3`) so a
/// future wire-format adapter can convert without changing the calculation
/// itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GroupingMode {
    /// A grouping mark is present when `grouping_id & mark == 0`.
    BitAnd = 1,
    /// A grouping mark is present when `grouping_id <= mark`.
    NumericCmp = 2,
    /// A grouping mark is present when `grouping_id` is absent from the set.
    NumericSet = 3,
}

impl TryFrom<u8> for GroupingMode {
    type Error = GroupingMetadataError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::BitAnd),
            2 => Ok(Self::NumericCmp),
            3 => Ok(Self::NumericSet),
            other => Err(GroupingMetadataError::InvalidMode(other)),
        }
    }
}

/// Errors raised while constructing or using grouping metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupingMetadataError {
    /// `GROUPING` was evaluated before planner metadata was installed.
    Uninitialized,
    /// A wire mode did not map to one of TiDB's supported algorithms.
    InvalidMode(u8),
    /// Bit-and and numeric-compare require exactly one mark per argument.
    InvalidGroupingMarkCount {
        /// The mode whose mark cardinality was invalid.
        mode: GroupingMode,
        /// Zero-based argument position of the invalid mark.
        index: usize,
        /// Number of grouping ids supplied for that argument.
        count: usize,
    },
}

/// Validated metadata attached to one scalar grouping function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingMetadata {
    mode: GroupingMode,
    grouping_marks: Vec<BTreeSet<u64>>,
}

impl GroupingMetadata {
    /// Validates and stores the source `GroupingMode` and grouping marks.
    pub fn new(
        mode: GroupingMode,
        grouping_marks: Vec<BTreeSet<u64>>,
    ) -> Result<Self, GroupingMetadataError> {
        if matches!(mode, GroupingMode::BitAnd | GroupingMode::NumericCmp) {
            for (index, mark) in grouping_marks.iter().enumerate() {
                if mark.len() != 1 {
                    return Err(GroupingMetadataError::InvalidGroupingMarkCount {
                        mode,
                        index,
                        count: mark.len(),
                    });
                }
            }
        }
        Ok(Self {
            mode,
            grouping_marks,
        })
    }

    /// Returns the selected source algorithm.
    pub fn mode(&self) -> GroupingMode {
        self.mode
    }

    /// Returns the validated mark sets in argument order.
    pub fn grouping_marks(&self) -> &[BTreeSet<u64>] {
        &self.grouping_marks
    }
}

/// Pure scalar implementation of TiDB's rewritten `GROUPING` function.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupingFunction {
    metadata: Option<GroupingMetadata>,
}

impl GroupingFunction {
    /// Constructs an uninitialized function, matching a freshly built Go
    /// `BuiltinGroupingImplSig` before `SetMetadata` runs.
    pub fn uninitialized() -> Self {
        Self::default()
    }

    /// Constructs a function with validated planner metadata.
    pub fn with_metadata(
        mode: GroupingMode,
        grouping_marks: Vec<BTreeSet<u64>>,
    ) -> Result<Self, GroupingMetadataError> {
        let mut function = Self::uninitialized();
        function.set_metadata(mode, grouping_marks)?;
        Ok(function)
    }

    /// Installs planner metadata.  A failed replacement leaves the function
    /// uninitialized, matching Go's `SetMetadata` failure state.
    pub fn set_metadata(
        &mut self,
        mode: GroupingMode,
        grouping_marks: Vec<BTreeSet<u64>>,
    ) -> Result<(), GroupingMetadataError> {
        self.metadata = None;
        let metadata = GroupingMetadata::new(mode, grouping_marks)?;
        self.metadata = Some(metadata);
        Ok(())
    }

    /// Returns validated metadata, or the source uninitialized error.
    pub fn metadata(&self) -> Result<&GroupingMetadata, GroupingMetadataError> {
        self.metadata
            .as_ref()
            .ok_or(GroupingMetadataError::Uninitialized)
    }

    /// Returns the selected mode, if metadata has been installed.
    pub fn mode(&self) -> Result<GroupingMode, GroupingMetadataError> {
        Ok(self.metadata()?.mode())
    }

    /// Evaluates one grouping id using the source algorithm.
    ///
    /// The Go signature returns an `int64` carrying an unsigned result flag;
    /// this typed leaf exposes the same bits directly as `u64`.
    pub fn eval(&self, grouping_id: u64) -> Result<u64, GroupingMetadataError> {
        let metadata = self.metadata()?;
        let mut result = 0u64;
        match metadata.mode {
            GroupingMode::BitAnd => {
                for mark in &metadata.grouping_marks {
                    result <<= 1;
                    let key = *mark
                        .iter()
                        .next()
                        .expect("validated bit-and mark has one element");
                    if grouping_id & key == 0 {
                        result += 1;
                    }
                }
            }
            GroupingMode::NumericCmp => {
                for mark in &metadata.grouping_marks {
                    result <<= 1;
                    let key = *mark
                        .iter()
                        .next()
                        .expect("validated numeric-compare mark has one element");
                    if grouping_id <= key {
                        result += 1;
                    }
                }
            }
            GroupingMode::NumericSet => {
                for mark in &metadata.grouping_marks {
                    result <<= 1;
                    if !mark.contains(&grouping_id) {
                        result += 1;
                    }
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marks(values: &[u64]) -> BTreeSet<u64> {
        values.iter().copied().collect()
    }

    /// Source rows from `pkg/expression/builtin_grouping_test.go:56
    /// TestGrouping`.  The Go implementation stores this result in an
    /// `int64` with the unsigned field flag; assertions compare the actual
    /// result bits as `u64` here.
    #[test]
    fn grouping_source_vectors() {
        let rows = [
            (1, GroupingMode::BitAnd, &[1][..], 0),
            (1, GroupingMode::BitAnd, &[3][..], 0),
            (1, GroupingMode::BitAnd, &[6][..], 1),
            (2, GroupingMode::BitAnd, &[1][..], 1),
            (2, GroupingMode::BitAnd, &[3][..], 0),
            (2, GroupingMode::BitAnd, &[6][..], 0),
            (4, GroupingMode::BitAnd, &[2][..], 1),
            (4, GroupingMode::BitAnd, &[4][..], 0),
            (4, GroupingMode::BitAnd, &[6][..], 0),
            (0, GroupingMode::NumericCmp, &[0][..], 1),
            (0, GroupingMode::NumericCmp, &[2][..], 1),
            (2, GroupingMode::NumericCmp, &[0][..], 0),
            (2, GroupingMode::NumericCmp, &[1][..], 0),
            (2, GroupingMode::NumericCmp, &[2][..], 1),
            (2, GroupingMode::NumericCmp, &[3][..], 1),
            (1, GroupingMode::NumericSet, &[1, 2][..], 0),
            (1, GroupingMode::NumericSet, &[2][..], 1),
            (2, GroupingMode::NumericSet, &[1, 3][..], 1),
            (2, GroupingMode::NumericSet, &[2, 3][..], 0),
        ];

        for (grouping_id, mode, grouping_ids, expected) in rows {
            let grouping = GroupingFunction::with_metadata(mode, vec![marks(grouping_ids)])
                .expect("source metadata is valid");
            assert_eq!(
                grouping.eval(grouping_id).unwrap(),
                expected,
                "mode={mode:?} id={grouping_id} marks={grouping_ids:?}"
            );
        }
    }

    #[test]
    fn metadata_validation_matches_source_guards() {
        assert_eq!(
            GroupingMode::try_from(0),
            Err(GroupingMetadataError::InvalidMode(0))
        );
        assert_eq!(
            GroupingMode::try_from(4),
            Err(GroupingMetadataError::InvalidMode(4))
        );

        for mode in [GroupingMode::BitAnd, GroupingMode::NumericCmp] {
            assert_eq!(
                GroupingMetadata::new(mode, vec![marks(&[])]),
                Err(GroupingMetadataError::InvalidGroupingMarkCount {
                    mode,
                    index: 0,
                    count: 0,
                })
            );
            assert_eq!(
                GroupingMetadata::new(mode, vec![marks(&[1, 2])]),
                Err(GroupingMetadataError::InvalidGroupingMarkCount {
                    mode,
                    index: 0,
                    count: 2,
                })
            );
        }

        // Numeric-set mode accepts an empty set: no grouping id is needed for
        // that argument, so every row is marked as grouped (`1`).
        let grouping =
            GroupingFunction::with_metadata(GroupingMode::NumericSet, vec![marks(&[])]).unwrap();
        assert_eq!(grouping.eval(0).unwrap(), 1);

        let mut uninitialized = GroupingFunction::uninitialized();
        assert_eq!(
            uninitialized.eval(1),
            Err(GroupingMetadataError::Uninitialized)
        );
        assert_eq!(
            uninitialized.metadata(),
            Err(GroupingMetadataError::Uninitialized)
        );
        assert!(uninitialized
            .set_metadata(GroupingMode::BitAnd, vec![marks(&[1, 2])])
            .is_err());
        assert_eq!(
            uninitialized.eval(1),
            Err(GroupingMetadataError::Uninitialized)
        );
    }
}
