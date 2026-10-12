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
//! [`tidb_domain::affinity`].

use tidb_domain::affinity::{AffinityGroupKeyRange, AffinityGroups};
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
        &tidb_codec::table_key::encode_table_prefix(physical_id),
    );
    tidb_codec::encode_bytes(
        &mut end_key,
        &tidb_codec::table_key::encode_table_prefix(physical_id + 1),
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

/// The PD affinity group work a DDL change owes: the groups it creates (Go
/// `createTableAffinityGroupsInPD`, which fails the job when PD refuses) and
/// the ones it deletes (Go `deleteTableAffinityGroupsInPD` /
/// `batchDeleteTableAffinityGroups`, best-effort cleanup).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AffinityGroupUpdate {
    /// The groups to create, idempotently.
    pub create: AffinityGroups,
    /// The prefix Go wraps a creation failure in, when it wraps one.
    pub create_error_context: Option<&'static str>,
    /// The group ids to delete.
    pub delete: Vec<String>,
}

impl AffinityGroupUpdate {
    /// Go `createTableAffinityGroupsInPD`: nothing for a table without
    /// affinity.
    pub fn create_for(&mut self, table: &TableInfo) -> Result<(), String> {
        self.create
            .extend(build_affinity_group_definitions(table, None)?);
        Ok(())
    }

    /// Go `deleteTableAffinityGroupsInPD`: the groups of `table`, or of the
    /// given partitions of it. A definition error is logged as Go's
    /// best-effort callers log it.
    pub fn delete_for(
        &mut self,
        table: &TableInfo,
        partition_defs: Option<&[PartitionDefinition]>,
    ) {
        match build_affinity_group_definitions(table, partition_defs) {
            Ok(groups) => self.delete.extend(groups.into_keys()),
            Err(error) => tidb_ddl_logutil::ddl_logger().error(
                "failed to delete affinity groups from PD",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                    tidb_log::Field::new("tableID", tidb_log::Value::I64(table.id)),
                ],
            ),
        }
    }

    /// Whether the change owes PD nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.create.is_empty() && self.delete.is_empty()
    }

    /// Creates the groups; the error is the job's.
    pub fn apply_create(&self) -> Result<(), String> {
        if self.create.is_empty() {
            return Ok(());
        }
        tidb_domain::affinity::create_groups_if_not_exists(&self.create).map_err(|error| match self
            .create_error_context
        {
            Some(context) => format!("{context}: {error}"),
            None => error.to_string(),
        })
    }

    /// Deletes the groups, best-effort with Go's retries; a final failure is
    /// logged and does not fail the change.
    pub fn apply_delete(&self) {
        if self.delete.is_empty() {
            return;
        }
        if let Err(error) = tidb_domain::affinity::delete_groups_with_retry(&self.delete) {
            tidb_ddl_logutil::ddl_logger().error(
                "failed to delete affinity groups from PD",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            );
        }
    }
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
                definitions: tidb_model::GoSharedSlice::from_vec(definitions),
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
