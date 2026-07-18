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

//! Dependency-closed partition metadata and ADD PARTITION job ordering.
//!
//! This leaf ports the metadata-only parts of `pkg/ddl/partition.go`:
//! partition-name validation, case-insensitive partition lookup, physical-ID
//! extraction, the `Definitions`/`AddingDefinitions` staging transition, and
//! the source ADD PARTITION schema-state order.  It deliberately does not
//! evaluate partition expressions, encode TiKV keys, allocate IDs, call PD,
//! mutate a catalog, or coordinate a DDL job.  Those effects remain explicit
//! owners for later Rust work.

// The metadata leaf is consumed by source-backed tests and a future catalog /
// DDL coordinator.  Keep it disconnected until those owners exist rather than
// inventing a fake physical-partition implementation.
#![allow(dead_code)]

use std::mem;

/// The source-owned identity fields needed by the metadata helpers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionDefinitionMetadata {
    /// Physical table ID assigned to this partition by the catalog owner.
    pub id: i64,
    /// Source-normalized partition name (`PartitionDefinition.Name.L`).
    pub name: String,
}

impl PartitionDefinitionMetadata {
    /// Creates a metadata record and applies the identifier's case-folding
    /// rule once, at the same boundary where Go stores `Name.L`.
    pub fn new(id: i64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: normalize_name(&name.into()),
        }
    }
}

/// Errors corresponding to the metadata validation branches in
/// `checkPartitionNameUnique`, `checkAddPartitionNameUnique`,
/// `checkReorgPartitionNames`, and `getPartitionDef`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PartitionMetadataError {
    /// Two definitions would occupy the same case-insensitive name.
    DuplicateName { name: String },
    /// A reorganize operation named a partition that is not present in the
    /// current definitions.
    MissingDroppedPartition { name: String },
    /// A replacement definition collides with a surviving definition or with
    /// an earlier replacement.
    ReplacementNameConflict { name: String },
    /// A lookup requested a partition absent from the table metadata.
    UnknownPartition { name: String },
}

/// The normalized key used by Go's `CIStr.L` comparisons in these helpers.
fn normalize_name(name: &str) -> String {
    name.to_lowercase()
}

/// Checks that one partition-definition list has unique normalized names.
///
/// This is the dependency-free behavior of Go's
/// `checkPartitionNameUnique`.  The first duplicate is reported in written
/// order, matching the source map probe.
pub fn check_partition_names_unique(
    definitions: &[PartitionDefinitionMetadata],
) -> Result<(), PartitionMetadataError> {
    let mut names = std::collections::BTreeSet::new();
    for definition in definitions {
        let name = normalize_name(&definition.name);
        if !names.insert(name.clone()) {
            return Err(PartitionMetadataError::DuplicateName { name });
        }
    }
    Ok(())
}

/// Checks that additions do not collide with existing definitions or with
/// one another, matching Go's `checkAddPartitionNameUnique`.
pub fn check_add_partition_names_unique(
    existing: &[PartitionDefinitionMetadata],
    additions: &[PartitionDefinitionMetadata],
) -> Result<(), PartitionMetadataError> {
    let mut names = std::collections::BTreeSet::new();
    for definition in existing.iter().chain(additions) {
        let name = normalize_name(&definition.name);
        if !names.insert(name.clone()) {
            return Err(PartitionMetadataError::DuplicateName { name });
        }
    }
    Ok(())
}

/// Validates a REORGANIZE operation's dropped and replacement names.
///
/// Go removes each dropped name from the current-name map before checking the
/// replacements.  That ordering is important: a replacement may reuse a name
/// that is being dropped, but cannot reuse a surviving name or an earlier
/// replacement.  A missing dropped name is reported before replacement
/// validation, as in `checkReorgPartitionNames`.
pub fn check_reorg_partition_names(
    existing: &[PartitionDefinitionMetadata],
    dropped_names: &[&str],
    replacements: &[PartitionDefinitionMetadata],
) -> Result<(), PartitionMetadataError> {
    let mut names: std::collections::BTreeSet<String> = existing
        .iter()
        .map(|definition| normalize_name(&definition.name))
        .collect();
    for dropped in dropped_names {
        let name = normalize_name(dropped);
        if !names.remove(&name) {
            return Err(PartitionMetadataError::MissingDroppedPartition { name });
        }
    }
    for replacement in replacements {
        let name = normalize_name(&replacement.name);
        if !names.insert(name.clone()) {
            return Err(PartitionMetadataError::ReplacementNameConflict { name });
        }
    }
    Ok(())
}

/// Finds a partition by case-insensitive name and preserves source order in
/// the returned index.  This is the metadata-only contract of `getPartitionDef`.
pub fn find_partition<'a>(
    definitions: &'a [PartitionDefinitionMetadata],
    name: &str,
) -> Result<(usize, &'a PartitionDefinitionMetadata), PartitionMetadataError> {
    let normalized = normalize_name(name);
    definitions
        .iter()
        .enumerate()
        .find(|(_, definition)| normalize_name(&definition.name) == normalized)
        .ok_or(PartitionMetadataError::UnknownPartition { name: normalized })
}

/// Extracts physical partition IDs without changing definition order.
pub fn partition_ids(definitions: &[PartitionDefinitionMetadata]) -> Vec<i64> {
    definitions.iter().map(|definition| definition.id).collect()
}

/// The source's transient ADD PARTITION schema states.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AddPartitionPhase {
    /// The job has not yet staged the additions (`StateNone`).
    Initial,
    /// Definitions are visible to replication/placement checks
    /// (`StateReplicaOnly`).
    ReplicaOnly,
    /// Definitions are published (`StatePublic`).
    Public,
}

impl AddPartitionPhase {
    /// Returns the exact source transition, or `None` for the terminal public
    /// state after the job has been finished.
    pub fn next(self) -> Option<Self> {
        match self {
            Self::Initial => Some(Self::ReplicaOnly),
            Self::ReplicaOnly => Some(Self::Public),
            Self::Public => None,
        }
    }

    /// Returns the complete source-ordered state sequence.
    pub const fn order() -> [Self; 3] {
        [Self::Initial, Self::ReplicaOnly, Self::Public]
    }
}

/// Metadata carried by an ADD PARTITION job before a real DDL coordinator
/// exists.  The IDs are immutable for this job and stay in source order.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AddPartitionJobMetadata {
    /// Logical table ID whose definitions are being extended.
    pub table_id: i64,
    /// Physical IDs of the definitions added by this job.
    pub partition_ids: Vec<i64>,
    /// Current source schema-state phase.
    pub phase: AddPartitionPhase,
}

impl AddPartitionJobMetadata {
    /// Creates an initial ADD PARTITION metadata record.
    pub fn new(table_id: i64, additions: &[PartitionDefinitionMetadata]) -> Self {
        Self {
            table_id,
            partition_ids: partition_ids(additions),
            phase: AddPartitionPhase::Initial,
        }
    }

    /// Advances one source schema-state phase and returns the new phase.  A
    /// finished (`Public`) job has no further transition.
    pub fn advance(&mut self) -> Option<AddPartitionPhase> {
        self.phase = self.phase.next()?;
        Some(self.phase)
    }
}

/// The source table metadata split used while ADD PARTITION runs.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PartitionTableMetadata {
    /// Public definitions, in original source order.
    pub definitions: Vec<PartitionDefinitionMetadata>,
    /// Definitions staged by the current ADD PARTITION job.
    pub adding_definitions: Vec<PartitionDefinitionMetadata>,
}

impl PartitionTableMetadata {
    /// Appends a job's definitions to `AddingDefinitions` in written order,
    /// matching `updateAddingPartitionInfo`.
    pub fn stage_additions(&mut self, additions: &[PartitionDefinitionMetadata]) {
        self.adding_definitions.extend_from_slice(additions);
    }

    /// Publishes staged definitions after the replica-only phase, preserving
    /// existing definitions before newly added ones and clearing the staging
    /// list, matching `updatePartitionInfo`.
    pub fn publish_additions(&mut self) {
        self.definitions
            .extend(mem::take(&mut self.adding_definitions));
    }
}
