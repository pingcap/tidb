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

//! Self-contained enums and constants from `pkg/meta/model/reorg.go`: the
//! backfill/reorg state and type enums and the reorg-meta version constants.
//!
//! Deferred to a later step (they depend on not-yet-ported pieces): the
//! `DDLReorgMeta` and `BackfillMeta` structs, whose accessors read
//! `vardef` runtime system variables, embed `TimeZoneLocation` (which loads
//! a time zone), and carry `terror.Error` warning maps.

/// Go `BackfillState` (a `byte`): the state of the backfill-merge process.
/// A newtype over `u8` so unknown values round-trip; [`Display`] falls
/// through to `"backfill state unknown"` like Go's `switch` default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackfillState(pub u8);

impl BackfillState {
    /// The backfill-merge process is not used (Go `BackfillStateInapplicable`,
    /// the zero value).
    pub const INAPPLICABLE: BackfillState = BackfillState(0);
    /// The backfill process is running (Go `BackfillStateRunning`).
    pub const RUNNING: BackfillState = BackfillState(1);
    /// The temporary index is ready to merge back (Go
    /// `BackfillStateReadyToMerge`).
    pub const READY_TO_MERGE: BackfillState = BackfillState(2);
    /// The temporary index is merging back (Go `BackfillStateMerging`).
    pub const MERGING: BackfillState = BackfillState(3);
}

impl std::fmt::Display for BackfillState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            BackfillState::RUNNING => "backfill state running",
            BackfillState::READY_TO_MERGE => "backfill state ready to merge",
            BackfillState::MERGING => "backfill state merging",
            BackfillState::INAPPLICABLE => "backfill state inapplicable",
            _ => "backfill state unknown",
        })
    }
}

/// Go `ReorgStage` (a `byte`): the stage of a reorganization, persisted to
/// reorg meta to avoid repeating completed work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReorgStage(pub u8);

impl ReorgStage {
    /// Not started (Go `ReorgStageNone`).
    pub const NONE: ReorgStage = ReorgStage(0);
    /// The column is being updated (Go `ReorgStageModifyColumnUpdateColumn`).
    pub const MODIFY_COLUMN_UPDATE_COLUMN: ReorgStage = ReorgStage(1);
    /// The index is being recreated (Go
    /// `ReorgStageModifyColumnRecreateIndex`).
    pub const MODIFY_COLUMN_RECREATE_INDEX: ReorgStage = ReorgStage(2);
    /// The reorganization is complete (Go `ReorgStageModifyColumnCompleted`).
    pub const MODIFY_COLUMN_COMPLETED: ReorgStage = ReorgStage(3);
}

/// Go `ReorgType` (an `int8`): the reorganization backend/strategy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReorgType(pub i8);

impl ReorgType {
    /// No reorganization (Go `ReorgTypeNone`, the zero value).
    pub const NONE: ReorgType = ReorgType(0);
    /// Transactional reorganization (Go `ReorgTypeTxn`).
    pub const TXN: ReorgType = ReorgType(1);
    /// Ingest (lightning) reorganization (Go `ReorgTypeIngest`).
    pub const INGEST: ReorgType = ReorgType(2);
    /// Transactional reorganization with a merge phase (Go
    /// `ReorgTypeTxnMerge`).
    pub const TXN_MERGE: ReorgType = ReorgType(3);

    /// Go `NeedMergeProcess`: whether this strategy has a temp-index merge.
    #[must_use]
    pub fn need_merge_process(self) -> bool {
        self == ReorgType::INGEST || self == ReorgType::TXN_MERGE
    }
}

impl std::fmt::Display for ReorgType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            ReorgType::TXN => "txn",
            ReorgType::INGEST => "ingest",
            ReorgType::TXN_MERGE => "txn-merge",
            // ReorgTypeNone and any unknown value.
            _ => "",
        })
    }
}

/// Go `ReorgMetaVersion0`: the minimum `DDLReorgMeta` version.
pub const REORG_META_VERSION0: i64 = 0;
/// Go `CurrentReorgMetaVersion`: the current `DDLReorgMeta` version.
pub const CURRENT_REORG_META_VERSION: i64 = 1;

/// The analyze-state values stored in `DDLReorgMeta.AnalyzeState`
/// (Go's `AnalyzeState*` constants).
pub mod analyze_state {
    /// Not started.
    pub const NONE: i8 = 0;
    /// Running.
    pub const RUNNING: i8 = 1;
    /// Skipped.
    pub const SKIPPED: i8 = 2;
    /// Done.
    pub const DONE: i8 = 3;
    /// Timed out.
    pub const TIMEOUT: i8 = 4;
    /// Failed.
    pub const FAILED: i8 = 5;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_state_strings() {
        assert_eq!(BackfillState::RUNNING.to_string(), "backfill state running");
        assert_eq!(
            BackfillState::READY_TO_MERGE.to_string(),
            "backfill state ready to merge"
        );
        assert_eq!(BackfillState::MERGING.to_string(), "backfill state merging");
        assert_eq!(
            BackfillState::INAPPLICABLE.to_string(),
            "backfill state inapplicable"
        );
        assert_eq!(BackfillState(9).to_string(), "backfill state unknown");
        assert_eq!(BackfillState::default(), BackfillState::INAPPLICABLE);
    }

    #[test]
    fn reorg_type_string_and_merge() {
        assert_eq!(ReorgType::NONE.to_string(), "");
        assert_eq!(ReorgType::TXN.to_string(), "txn");
        assert_eq!(ReorgType::INGEST.to_string(), "ingest");
        assert_eq!(ReorgType::TXN_MERGE.to_string(), "txn-merge");

        assert!(!ReorgType::NONE.need_merge_process());
        assert!(!ReorgType::TXN.need_merge_process());
        assert!(ReorgType::INGEST.need_merge_process());
        assert!(ReorgType::TXN_MERGE.need_merge_process());
    }

    #[test]
    fn versions_and_stage() {
        assert_eq!(REORG_META_VERSION0, 0);
        assert_eq!(CURRENT_REORG_META_VERSION, 1);
        assert_eq!(ReorgStage::default(), ReorgStage::NONE);
        assert_eq!(analyze_state::FAILED, 5);
    }
}
