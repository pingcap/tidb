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

//! Partition DDL execution boundary.

use tidb_ast::{AlterPartitionAction, AlterTableAction};

/// Returns the pre-mutation error for every partition action unsupported by
/// this seed catalog. A future partition catalog changes this single boundary
/// rather than leaving mismatched checks in both DDL dispatch layers.
pub(crate) fn unsupported_alter_action(action: &AlterTableAction) -> Option<&'static str> {
    match action {
        AlterTableAction::Partition(AlterPartitionAction::SetPlacementPolicy { .. }) => {
            Some("ALTER TABLE PARTITION PLACEMENT POLICY")
        }
        AlterTableAction::Partition(AlterPartitionAction::Add { .. }) => {
            Some("ALTER TABLE ADD PARTITION")
        }
        AlterTableAction::Partition(AlterPartitionAction::Exchange { .. }) => {
            Some("ALTER TABLE EXCHANGE PARTITION")
        }
        AlterTableAction::Partition(AlterPartitionAction::Drop { .. }) => {
            Some("ALTER TABLE DROP PARTITION")
        }
        AlterTableAction::Partition(AlterPartitionAction::Check { .. }) => {
            Some("ALTER TABLE CHECK PARTITION")
        }
        AlterTableAction::Partition(AlterPartitionAction::ImportTablespace { .. }) => {
            Some("ALTER TABLE IMPORT PARTITION TABLESPACE")
        }
        AlterTableAction::Partition(AlterPartitionAction::DiscardTablespace { .. }) => {
            Some("ALTER TABLE DISCARD PARTITION TABLESPACE")
        }
        AlterTableAction::Partition(
            AlterPartitionAction::FirstPartitionLessThan { .. }
            | AlterPartitionAction::LastPartitionLessThan { .. },
        ) => Some("ALTER TABLE INTERVAL PARTITION BOUND"),
        AlterTableAction::Partition(AlterPartitionAction::SplitMaxValuePartition { .. }) => {
            Some("ALTER TABLE SPLIT MAXVALUE PARTITION")
        }
        AlterTableAction::Partition(AlterPartitionAction::MergeFirstPartitionLessThan {
            ..
        }) => Some("ALTER TABLE MERGE FIRST PARTITION"),
        AlterTableAction::Partition(_) => Some("ALTER TABLE partition maintenance"),
        _ => None,
    }
}
