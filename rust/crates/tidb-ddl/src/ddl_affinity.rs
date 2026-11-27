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

//! Go `pkg/ddl/affinity.go`: the PD affinity groups a table's `AFFINITY`
//! option stands for, which the DDL job workers create and delete through
//! [`tidb_domain_affinity`].

use tidb_domain_affinity::{AffinityGroupKeyRange, AffinityGroups};
use tidb_model::{PartitionDefinition, TableInfo};

/// Go `GetTableAffinityGroupID`: `_tidb_t_{tableID}`.
#[must_use]
pub fn get_table_affinity_group_id(table_id: i64) -> String {
    format!("_tidb_t_{table_id}")
}

/// Go `GetPartitionAffinityGroupID`: `_tidb_pt_{tableID}_p{partitionID}`.
#[must_use]
pub fn get_partition_affinity_group_id(table_id: i64, partition_id: i64) -> String {
    format!("_tidb_pt_{table_id}_p{partition_id}")
}

/// Go `buildAffinityGroupKeyRange`: the physical table's whole keyspace, in
/// the store codec's region encoding (the classic codec memcomparable-encodes
/// both bounds).
fn build_affinity_group_key_range(physical_id: i64) -> AffinityGroupKeyRange {
    let mut start_key = Vec::new();
    let mut end_key = Vec::new();
    tidb_codec::encode_bytes(
        &mut start_key,
        &tidb_tablecodec::table_key::encode_table_prefix(physical_id),
    );
    tidb_codec::encode_bytes(
        &mut end_key,
        &tidb_tablecodec::table_key::encode_table_prefix(physical_id + 1),
    );
    AffinityGroupKeyRange { start_key, end_key }
}

/// Go `buildAffinityGroupDefinitions`: one group for a `table`-level table,
/// one per partition for a `partition`-level one. `partition_defs` names the
/// partitions to cover; `None` reads the table's own.
pub fn build_affinity_group_definitions(
    table: &TableInfo,
    partition_defs: Option<&[PartitionDefinition]>,
) -> Result<AffinityGroups, String> {
    let Some(affinity) = table.affinity.as_ref() else {
        return Ok(AffinityGroups::new());
    };
    let level = affinity.read().level.clone();
    match level.as_str() {
        // Go `ast.TableAffinityLevelTable`.
        "table" => Ok([(
            get_table_affinity_group_id(table.id),
            vec![build_affinity_group_key_range(table.id)],
        )]
        .into_iter()
        .collect()),
        // Go `ast.TableAffinityLevelPartition`.
        "partition" => {
            let definitions: Vec<PartitionDefinition> = match partition_defs {
                Some(definitions) => definitions.to_vec(),
                None => table
                    .get_partition_info()
                    .map(|partition| partition.read().definitions.snapshot())
                    .unwrap_or_default(),
            };
            if definitions.is_empty() {
                return Err(format!(
                    "partition affinity requires partition definitions for table {} (ID: {}), \
                     table metadata may be corrupted",
                    table.name.original(),
                    table.id
                ));
            }
            Ok(definitions
                .iter()
                .map(|definition| {
                    (
                        get_partition_affinity_group_id(table.id, definition.id),
                        vec![build_affinity_group_key_range(definition.id)],
                    )
                })
                .collect())
        }
        other => Err(format!(
            "invalid affinity level: {other} for table {} (ID: {})",
            table.name.original(),
            table.id
        )),
    }
}

/// Go `collectAffinityGroupIDs`.
fn collect_affinity_group_ids(groups: &AffinityGroups) -> Vec<String> {
    groups.keys().cloned().collect()
}

/// Go `createTableAffinityGroupsInPD`: creates the groups of `table` in PD,
/// of the given partitions or, with none, of all its partitions. A critical
/// operation: the caller fails the DDL when it fails. Used by CREATE TABLE,
/// ALTER TABLE AFFINITY = 'xxx', TRUNCATE TABLE and TRUNCATE PARTITION.
pub fn create_table_affinity_groups_in_pd(
    table: &TableInfo,
    partition_defs: &[PartitionDefinition],
) -> Result<(), String> {
    if table.affinity.is_none() {
        return Ok(());
    }
    let partition_defs = (!partition_defs.is_empty()).then_some(partition_defs);
    let groups = build_affinity_group_definitions(table, partition_defs)?;
    if groups.is_empty() {
        return Ok(());
    }
    tidb_ddl_logutil::ddl_logger().info(
        "creating affinity groups in PD",
        &[
            tidb_log::Field::new("tableID", tidb_log::Value::I64(table.id)),
            tidb_log::Field::new(
                "groupIDs",
                tidb_log::Value::Str(format!("{:?}", collect_affinity_group_ids(&groups))),
            ),
        ],
    );
    tidb_domain_affinity::create_groups_if_not_exists(&groups).map_err(|error| error.to_string())
}

/// Go `deleteTableAffinityGroupsInPD`: deletes the groups of `table`, or of
/// the given partitions of it. Best-effort cleanup: the callers log a
/// failure and continue.
pub fn delete_table_affinity_groups_in_pd(
    table: &TableInfo,
    partition_defs: Option<&[PartitionDefinition]>,
) -> Result<(), String> {
    delete_table_affinity_groups(table, partition_defs)
}

/// Go `deleteTableAffinityGroupsWithTimeout`: the deletion rollback and
/// post-commit cleanup run, after the step context may be cancelled. These
/// PD calls take no context, so it is [`delete_table_affinity_groups_in_pd`].
pub fn delete_table_affinity_groups_with_timeout(
    table: &TableInfo,
    partition_defs: Option<&[PartitionDefinition]>,
) -> Result<(), String> {
    delete_table_affinity_groups(table, partition_defs)
}

/// Go `deleteTableAffinityGroups`.
fn delete_table_affinity_groups(
    table: &TableInfo,
    partition_defs: Option<&[PartitionDefinition]>,
) -> Result<(), String> {
    let groups = build_affinity_group_definitions(table, partition_defs)?;
    tidb_domain_affinity::delete_groups_with_retry(&collect_affinity_group_ids(&groups))
        .map_err(|error| error.to_string())
}

/// Go `partitionDefinitionsFromIDs`: an explicit list even when `ids` is
/// empty, since no list selects all of a table's partitions.
#[must_use]
pub fn partition_definitions_from_ids(ids: &[i64]) -> Vec<PartitionDefinition> {
    ids.iter()
        .map(|id| PartitionDefinition {
            id: *id,
            ..PartitionDefinition::default()
        })
        .collect()
}

/// Go `collectTableAffinityGroupIDs`: adds the group ids of `table` to
/// `group_ids`.
pub fn collect_table_affinity_group_ids(
    table: &TableInfo,
    group_ids: &mut std::collections::BTreeSet<String>,
) -> Result<(), String> {
    group_ids.extend(build_affinity_group_definitions(table, None)?.into_keys());
    Ok(())
}

/// Go `batchDeleteTableAffinityGroups`: deletes the groups of several
/// tables (DROP DATABASE); the error is the caller's to log.
pub fn batch_delete_table_affinity_groups(
    group_ids: &std::collections::BTreeSet<String>,
) -> Result<(), String> {
    if group_ids.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = group_ids.iter().cloned().collect();
    tidb_ddl_logutil::ddl_logger().info(
        "deleting affinity groups for batch tables",
        &[tidb_log::Field::new("groupIDs", tidb_log::Value::Str(format!("{ids:?}")))],
    );
    tidb_domain_affinity::delete_groups_with_retry(&ids).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(id: i64, level: &str, partitions: &[i64]) -> TableInfo {
        let mut table = TableInfo {
            id,
            name: tidb_ast::CiString::new("t".to_owned()),
            ..TableInfo::default()
        };
        table.affinity = tidb_model::new_table_affinity_info_with_level(level)
            .unwrap()
            .map(tidb_model::GoShared::new);
        if !partitions.is_empty() {
            let definitions = partitions
                .iter()
                .map(|id| PartitionDefinition {
                    id: *id,
                    ..PartitionDefinition::default()
                })
                .collect();
            table.partition = Some(tidb_model::GoShared::new(tidb_model::PartitionInfo {
                enable: true,
                definitions: tidb_goruntime::go_runtime::GoSharedSlice::from_vec(definitions),
                ..tidb_model::PartitionInfo::default()
            }));
        }
        table
    }

    #[test]
    fn table_level_is_one_group_over_the_table() {
        let groups = build_affinity_group_definitions(&table(42, "table", &[]), None).unwrap();
        assert_eq!(groups.keys().collect::<Vec<_>>(), ["_tidb_t_42"]);
        assert_eq!(
            groups["_tidb_t_42"],
            vec![build_affinity_group_key_range(42)]
        );
    }

    #[test]
    fn partition_level_is_one_group_per_partition() {
        let groups =
            build_affinity_group_definitions(&table(42, "partition", &[43, 44]), None).unwrap();
        assert_eq!(
            groups.keys().collect::<Vec<_>>(),
            ["_tidb_pt_42_p43", "_tidb_pt_42_p44"]
        );
        let only = [PartitionDefinition {
            id: 44,
            ..PartitionDefinition::default()
        }];
        let groups =
            build_affinity_group_definitions(&table(42, "partition", &[43, 44]), Some(&only))
                .unwrap();
        assert_eq!(groups.keys().collect::<Vec<_>>(), ["_tidb_pt_42_p44"]);
    }

    #[test]
    fn no_affinity_is_no_group_and_partition_level_needs_partitions() {
        assert!(
            build_affinity_group_definitions(&TableInfo::default(), None)
                .unwrap()
                .is_empty()
        );
        assert!(build_affinity_group_definitions(&table(42, "partition", &[]), None).is_err());
    }
}
