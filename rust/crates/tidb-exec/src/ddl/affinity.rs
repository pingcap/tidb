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

//! Dependency-closed table-affinity metadata.
//!
//! This is the deterministic part of `pkg/ddl/affinity.go`: normalize the
//! source affinity level, derive the stable table/partition group IDs, and
//! validate that partition affinity has physical definitions.  It does not
//! encode TiKV keys, call PD, mutate catalog metadata, or own DDL commit
//! ordering.  Those operations remain an explicit coordinator boundary until
//! the Rust catalog and TiKV/PD owners exist.

// This metadata leaf is intentionally consumed by source-backed tests and the
// future DDL/PD coordinator; keeping it disconnected today avoids inventing a
// fake catalog or transport path while the rewrite is still partial.
#![allow(dead_code)]

use std::collections::BTreeMap;

/// The source affinity levels accepted by `NormalizeTableAffinityLevel`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AffinityLevel {
    /// No affinity metadata is published (`none` or an empty literal).
    None,
    /// One affinity group covers the table's physical ID.
    Table,
    /// One affinity group is derived for every partition physical ID.
    Partition,
}

/// A source-shaped affinity-level normalization error.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvalidAffinityLevel {
    /// The literal that was rejected.
    pub level: String,
}

/// Errors raised while deriving deterministic affinity metadata.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AffinityMetadataError {
    /// The source requested a level other than `none`, `table`, or
    /// `partition`.
    InvalidLevel {
        /// The table physical ID used to identify the source metadata.
        table_id: i64,
        /// The original source literal.
        level: String,
    },
    /// Partition affinity cannot produce groups without partition physical
    /// IDs.  This mirrors `buildAffinityGroupDefinitions`'s corruption guard.
    MissingPartitionDefinitions {
        /// The table physical ID used to identify the source metadata.
        table_id: i64,
    },
}

/// The deterministic identity of one source affinity group.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AffinityGroupMetadata {
    /// The stable group ID sent to the placement service by the later PD
    /// owner.
    pub group_id: String,
    /// The table or partition physical ID whose key range will be encoded by
    /// the later tablecodec owner.
    pub physical_id: i64,
}

/// Normalizes an affinity literal exactly like Go's
/// `ast.NormalizeTableAffinityLevel`.
pub fn normalize_level(level: &str) -> Result<AffinityLevel, InvalidAffinityLevel> {
    match level.to_lowercase().as_str() {
        "" | "none" => Ok(AffinityLevel::None),
        "table" => Ok(AffinityLevel::Table),
        "partition" => Ok(AffinityLevel::Partition),
        _ => Err(InvalidAffinityLevel {
            level: level.to_owned(),
        }),
    }
}

/// Returns the stable group ID used by Go's table-affinity DDL helpers.
pub fn table_group_id(table_id: i64) -> String {
    format!("_tidb_t_{table_id}")
}

/// Returns the stable group ID used by Go's partition-affinity DDL helpers.
pub fn partition_group_id(table_id: i64, partition_id: i64) -> String {
    format!("_tidb_pt_{table_id}_p{partition_id}")
}

/// Builds source-shaped affinity group identities without physical key bytes.
///
/// `None` means the table has no `TableAffinityInfo`; `Some("")` and
/// `Some("none")` are the source's explicit no-affinity forms.  Duplicate
/// partition IDs collapse as they do in Go's map-backed group definitions,
/// and the returned list is sorted by group ID for reproducible evidence.
pub fn build_group_metadata(
    level: Option<&str>,
    table_id: i64,
    partition_ids: &[i64],
) -> Result<Vec<AffinityGroupMetadata>, AffinityMetadataError> {
    let Some(level) = level else {
        return Ok(Vec::new());
    };
    let normalized =
        normalize_level(level).map_err(|invalid| AffinityMetadataError::InvalidLevel {
            table_id,
            level: invalid.level,
        })?;

    let mut groups = BTreeMap::new();
    match normalized {
        AffinityLevel::None => {}
        AffinityLevel::Table => {
            groups.insert(
                table_group_id(table_id),
                AffinityGroupMetadata {
                    group_id: table_group_id(table_id),
                    physical_id: table_id,
                },
            );
        }
        AffinityLevel::Partition => {
            if partition_ids.is_empty() {
                return Err(AffinityMetadataError::MissingPartitionDefinitions { table_id });
            }
            for &partition_id in partition_ids {
                let group_id = partition_group_id(table_id, partition_id);
                groups.insert(
                    group_id.clone(),
                    AffinityGroupMetadata {
                        group_id,
                        physical_id: partition_id,
                    },
                );
            }
        }
    }
    Ok(groups.into_values().collect())
}
