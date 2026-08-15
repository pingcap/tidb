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

//! Complete transcreation of Go `pkg/infoschema/context`
//! (`infoschema.go`): the special-attribute predicates that
//! `ListTablesWithSpecialAttribute` filters tables by, its paired result, and
//! the schema/table iteration contract.
//!
//! Go's `MetaOnlyInfoSchema` and `Misc` are not reproduced as traits, and
//! their own doc comment says why: "MetaOnlyInfoSchema is a workaround. Due
//! to circular dependency cannot return the complete interface." They exist
//! so `pkg/infoschema` can hand a narrowed view to packages it would
//! otherwise import cyclically. Rust has no such cycle — a catalog type
//! exposes those methods directly — so reproducing the workaround would add
//! traits with no implementor and no observable behavior. [`SchemaAndTable`]
//! *is* reproduced, because Go gives it a real implementor
//! ([`DbInfoAsInfoSchema`]) whose lookup this module owns.

use tidb_ast::CiString;
use tidb_model::db::DBInfo;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;

/// Go `SpecialAttributeFilter`: a predicate selecting tables that carry one
/// special attribute.
pub type SpecialAttributeFilter = fn(&TableInfo) -> bool;

/// Go `TTLAttribute`. Unlike its siblings this one also requires the table to
/// be public, so a table whose TTL is still being created is not listed.
#[must_use]
pub fn ttl_attribute(table: &TableInfo) -> bool {
    table.state == SchemaState::PUBLIC && table.ttl_info.is_some()
}

/// Go `TiFlashAttribute`.
#[must_use]
pub fn tiflash_attribute(table: &TableInfo) -> bool {
    table.tiflash_replica.is_some()
}

/// Go `PlacementPolicyAttribute`: the table's own policy, or any partition's.
///
/// Partitions are read through `GetPartitionInfo`, so a table whose partition
/// info is disabled contributes no partition policies.
#[must_use]
pub fn placement_policy_attribute(table: &TableInfo) -> bool {
    if table.placement_policy_ref.is_some() {
        return true;
    }
    table.get_partition_info().is_some_and(|partition| {
        partition
            .read()
            .definitions
            .map_visible(|definition| definition.placement_policy_ref.is_some())
            .into_iter()
            .any(|has_policy| has_policy)
    })
}

/// Go `AllPlacementPolicyAttribute`: like [`placement_policy_attribute`] but
/// reading `Partition` directly, so the partition `Enable` flag is ignored.
#[must_use]
pub fn all_placement_policy_attribute(table: &TableInfo) -> bool {
    if table.placement_policy_ref.is_some() {
        return true;
    }
    table.partition.as_ref().is_some_and(|partition| {
        partition
            .read()
            .definitions
            .map_visible(|definition| definition.placement_policy_ref.is_some())
            .into_iter()
            .any(|has_policy| has_policy)
    })
}

/// Go `TableLockAttribute`.
#[must_use]
pub fn table_lock_attribute(table: &TableInfo) -> bool {
    table.lock.is_some()
}

/// Go `PartitionAttribute`: partitioned *and* enabled, via `GetPartitionInfo`.
#[must_use]
pub fn partition_attribute(table: &TableInfo) -> bool {
    table.get_partition_info().is_some()
}

/// Go `AffinityAttribute`.
#[must_use]
pub fn affinity_attribute(table: &TableInfo) -> bool {
    table.affinity.is_some()
}

/// Go `HasSpecialAttributes`.
///
/// Note the source's own set: it checks TTL, TiFlash, placement policy,
/// partition, table lock, and affinity — and deliberately uses
/// `PlacementPolicyAttribute`, not the `All` variant, so a disabled
/// partition's policies do not make a table special.
#[must_use]
pub fn has_special_attributes(table: &TableInfo) -> bool {
    ttl_attribute(table)
        || tiflash_attribute(table)
        || placement_policy_attribute(table)
        || partition_attribute(table)
        || table_lock_attribute(table)
        || affinity_attribute(table)
}

/// Go `AllSpecialAttribute`, the filter form of [`has_special_attributes`].
pub const ALL_SPECIAL_ATTRIBUTE: SpecialAttributeFilter = has_special_attributes;

/// Go `TableInfoResult`: one schema's matching tables.
#[derive(Clone, Debug, Default)]
pub struct TableInfoResult {
    /// The schema these tables belong to.
    pub db_name: CiString,
    /// Tables in the schema that passed the filter.
    pub table_infos: Vec<TableInfo>,
}

/// Go `SchemaAndTable`: iteration over every schema and its tables.
pub trait SchemaAndTable {
    /// Go `AllSchemas`.
    fn all_schemas(&self) -> Vec<&DBInfo>;

    /// Go `SchemaTableInfos`: the schema's tables, or none when the schema is
    /// unknown — Go returns a nil slice and a nil error for a missing schema
    /// rather than reporting it.
    fn schema_table_infos(&self, schema: &CiString) -> Vec<TableInfo>;
}

/// Go `DBInfoAsInfoSchema`, a slice of schemas viewed as an info schema.
#[derive(Clone, Debug, Default)]
pub struct DbInfoAsInfoSchema(pub Vec<DBInfo>);

impl SchemaAndTable for DbInfoAsInfoSchema {
    fn all_schemas(&self) -> Vec<&DBInfo> {
        self.0.iter().collect()
    }

    fn schema_table_infos(&self, schema: &CiString) -> Vec<TableInfo> {
        for db in &self.0 {
            if db.name == *schema {
                return db
                    .deprecated_tables
                    .iter_deref()
                    .map(|table| table.read().clone())
                    .collect();
            }
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};
    use tidb_model::placement::PolicyRefInfo;
    use tidb_model::table::{TTLInfo, TableAffinityInfo, TableLockInfo, TiFlashReplicaInfo};
    use tidb_model::GoShared;

    fn table() -> TableInfo {
        TableInfo {
            state: SchemaState::PUBLIC,
            ..Default::default()
        }
    }

    fn policy_ref() -> GoShared<PolicyRefInfo> {
        GoShared::new(PolicyRefInfo::default())
    }

    fn partition_with_policy(enable: bool) -> GoShared<PartitionInfo> {
        GoShared::new(PartitionInfo {
            enable,
            definitions: vec![PartitionDefinition {
                placement_policy_ref: Some(policy_ref()),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        })
    }

    // A plain public table carries no special attribute at all.
    #[test]
    fn a_plain_table_matches_no_filter() {
        let plain = table();
        for filter in [
            ttl_attribute as SpecialAttributeFilter,
            tiflash_attribute,
            placement_policy_attribute,
            all_placement_policy_attribute,
            table_lock_attribute,
            partition_attribute,
            affinity_attribute,
            ALL_SPECIAL_ATTRIBUTE,
        ] {
            assert!(!filter(&plain));
        }
    }

    // Go's TTL filter is the only one that also gates on the schema state.
    #[test]
    fn ttl_requires_a_public_table() {
        let mut ttl = table();
        ttl.ttl_info = Some(GoShared::new(TTLInfo::default()));
        assert!(ttl_attribute(&ttl));
        assert!(has_special_attributes(&ttl));

        ttl.state = SchemaState::WRITE_ONLY;
        assert!(!ttl_attribute(&ttl));
        assert!(!has_special_attributes(&ttl));
    }

    // The single-field filters each answer for their own field only.
    #[test]
    fn single_field_filters_are_independent() {
        let mut tiflash = table();
        tiflash.tiflash_replica = Some(GoShared::new(TiFlashReplicaInfo::default()));
        assert!(tiflash_attribute(&tiflash));
        assert!(!table_lock_attribute(&tiflash));
        assert!(has_special_attributes(&tiflash));

        let mut locked = table();
        locked.lock = Some(GoShared::new(TableLockInfo::default()));
        assert!(table_lock_attribute(&locked));
        assert!(!tiflash_attribute(&locked));
        assert!(has_special_attributes(&locked));

        let mut affine = table();
        affine.affinity = Some(GoShared::new(TableAffinityInfo::default()));
        assert!(affinity_attribute(&affine));
        assert!(has_special_attributes(&affine));
    }

    // A table's own policy satisfies both placement filters regardless of any
    // partition state.
    #[test]
    fn a_table_policy_satisfies_both_placement_filters() {
        let mut owned = table();
        owned.placement_policy_ref = Some(policy_ref());
        assert!(placement_policy_attribute(&owned));
        assert!(all_placement_policy_attribute(&owned));
    }

    // This is the pair's whole point: a disabled partition's policies are
    // invisible to `PlacementPolicyAttribute` but visible to the `All` form.
    #[test]
    fn a_disabled_partition_hides_its_policies_from_only_one_filter() {
        let mut enabled = table();
        enabled.partition = Some(partition_with_policy(true));
        assert!(placement_policy_attribute(&enabled));
        assert!(all_placement_policy_attribute(&enabled));
        assert!(partition_attribute(&enabled));
        assert!(has_special_attributes(&enabled));

        let mut disabled = table();
        disabled.partition = Some(partition_with_policy(false));
        assert!(!placement_policy_attribute(&disabled));
        assert!(all_placement_policy_attribute(&disabled));
        assert!(!partition_attribute(&disabled));
        // `HasSpecialAttributes` uses the non-`All` filter, so a disabled
        // partition leaves the table unremarkable.
        assert!(!has_special_attributes(&disabled));
    }

    // A partition with no policy is still a partition.
    #[test]
    fn a_policyless_partition_is_a_partition_but_not_a_placement() {
        let mut partitioned = table();
        partitioned.partition = Some(GoShared::new(PartitionInfo {
            enable: true,
            definitions: vec![PartitionDefinition::default()].into(),
            ..Default::default()
        }));
        assert!(partition_attribute(&partitioned));
        assert!(!placement_policy_attribute(&partitioned));
        assert!(!all_placement_policy_attribute(&partitioned));
        assert!(has_special_attributes(&partitioned));
    }

    // Go `DBInfoAsInfoSchema`: name lookup is case-insensitive through CIStr,
    // and an unknown schema yields nothing rather than an error.
    #[test]
    fn db_info_as_info_schema_looks_schemas_up_by_name() {
        let mut named = table();
        named.id = 7;
        let db = DBInfo {
            name: CiString::new("Test"),
            deprecated_tables: vec![named].into(),
            ..Default::default()
        };
        let schema = DbInfoAsInfoSchema(vec![db]);

        assert_eq!(schema.all_schemas().len(), 1);
        let tables = schema.schema_table_infos(&CiString::new("test"));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].id, 7);
        assert!(schema
            .schema_table_infos(&CiString::new("missing"))
            .is_empty());
    }
}
