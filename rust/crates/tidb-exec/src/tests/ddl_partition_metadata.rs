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

//! Source-backed tests for the metadata-only partition DDL leaf.

use crate::ddl::partition_metadata::{
    check_add_partition_names_unique, check_partition_names_unique, check_reorg_partition_names,
    find_partition, partition_ids, AddPartitionJobMetadata, AddPartitionPhase,
    PartitionDefinitionMetadata, PartitionMetadataError, PartitionTableMetadata,
};

fn definition(id: i64, name: &str) -> PartitionDefinitionMetadata {
    PartitionDefinitionMetadata::new(id, name)
}

#[test]
fn source_partition_names_are_case_insensitive_and_ordered() {
    let definitions = [definition(11, "P0"), definition(12, "p1")];
    assert_eq!(check_partition_names_unique(&definitions), Ok(()));
    assert_eq!(partition_ids(&definitions), vec![11, 12]);
    assert_eq!(find_partition(&definitions, "p0"), Ok((0, &definitions[0])));
    assert_eq!(find_partition(&definitions, "P1"), Ok((1, &definitions[1])));
    assert_eq!(
        find_partition(&definitions, "missing"),
        Err(PartitionMetadataError::UnknownPartition {
            name: "missing".to_owned()
        })
    );
}

#[test]
fn source_partition_name_validation_covers_add_and_reorganize_order() {
    let existing = [definition(1, "p0"), definition(2, "p1")];
    assert_eq!(
        check_partition_names_unique(&[definition(1, "p0"), definition(2, "P0")]),
        Err(PartitionMetadataError::DuplicateName {
            name: "p0".to_owned()
        })
    );
    assert_eq!(
        check_add_partition_names_unique(&existing, &[definition(3, "P2")]),
        Ok(())
    );
    assert_eq!(
        check_add_partition_names_unique(&existing, &[definition(3, "P1")]),
        Err(PartitionMetadataError::DuplicateName {
            name: "p1".to_owned()
        })
    );
    assert_eq!(
        check_reorg_partition_names(&existing, &["P0"], &[definition(4, "p0")]),
        Ok(())
    );
    assert_eq!(
        check_reorg_partition_names(&existing, &["missing"], &[definition(4, "p2")]),
        Err(PartitionMetadataError::MissingDroppedPartition {
            name: "missing".to_owned()
        })
    );
    assert_eq!(
        check_reorg_partition_names(&existing, &["p0"], &[definition(4, "p1")]),
        Err(PartitionMetadataError::ReplacementNameConflict {
            name: "p1".to_owned()
        })
    );
}

#[test]
fn source_add_partition_stages_then_publishes_in_source_order() {
    assert_eq!(
        AddPartitionPhase::order(),
        [
            AddPartitionPhase::Initial,
            AddPartitionPhase::ReplicaOnly,
            AddPartitionPhase::Public
        ]
    );
    let additions = [definition(20, "p2"), definition(21, "p3")];
    let mut job = AddPartitionJobMetadata::new(7, &additions);
    assert_eq!(job.partition_ids, vec![20, 21]);
    assert_eq!(job.advance(), Some(AddPartitionPhase::ReplicaOnly));
    assert_eq!(job.advance(), Some(AddPartitionPhase::Public));
    assert_eq!(job.advance(), None);

    let mut table = PartitionTableMetadata {
        definitions: vec![definition(10, "p0"), definition(11, "p1")],
        adding_definitions: Vec::new(),
    };
    table.stage_additions(&additions);
    assert_eq!(partition_ids(&table.definitions), vec![10, 11]);
    assert_eq!(partition_ids(&table.adding_definitions), vec![20, 21]);
    table.publish_additions();
    assert_eq!(partition_ids(&table.definitions), vec![10, 11, 20, 21]);
    assert!(table.adding_definitions.is_empty());
}
