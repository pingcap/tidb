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

//! Source-backed tests for the dependency-closed part of `pkg/ddl/affinity.go`.

use crate::ddl::affinity::{
    build_group_metadata, normalize_level, partition_group_id, table_group_id,
    AffinityGroupMetadata, AffinityLevel, AffinityMetadataError, InvalidAffinityLevel,
};

#[test]
fn source_affinity_level_normalization_and_group_identity() {
    assert_eq!(normalize_level("TABLE"), Ok(AffinityLevel::Table));
    assert_eq!(normalize_level("none"), Ok(AffinityLevel::None));
    assert_eq!(normalize_level(""), Ok(AffinityLevel::None));
    assert_eq!(normalize_level("PARTITION"), Ok(AffinityLevel::Partition));
    assert_eq!(
        normalize_level("invalid_affinity"),
        Err(InvalidAffinityLevel {
            level: "invalid_affinity".to_owned(),
        })
    );
    assert_eq!(table_group_id(123), "_tidb_t_123");
    assert_eq!(partition_group_id(50, 3), "_tidb_pt_50_p3");
}

#[test]
fn source_table_affinity_definition_is_single_group() {
    assert_eq!(
        build_group_metadata(Some("table"), 123, &[1, 2]),
        Ok(vec![AffinityGroupMetadata {
            group_id: "_tidb_t_123".to_owned(),
            physical_id: 123,
        }])
    );
}

#[test]
fn source_partition_affinity_definition_requires_partitions() {
    assert_eq!(
        build_group_metadata(Some("partition"), 50, &[3, 1, 3]),
        Ok(vec![
            AffinityGroupMetadata {
                group_id: "_tidb_pt_50_p1".to_owned(),
                physical_id: 1,
            },
            AffinityGroupMetadata {
                group_id: "_tidb_pt_50_p3".to_owned(),
                physical_id: 3,
            },
        ])
    );
    assert_eq!(
        build_group_metadata(Some("partition"), 1, &[]),
        Err(AffinityMetadataError::MissingPartitionDefinitions { table_id: 1 })
    );
    assert_eq!(build_group_metadata(None, 1, &[]), Ok(Vec::new()));
    assert_eq!(build_group_metadata(Some("none"), 1, &[7]), Ok(Vec::new()));
}
