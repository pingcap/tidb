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

//! `ALTER TABLE ... [PARTITION p] ATTRIBUTES` and the label-rule upkeep of
//! Go's DDL job workers: every job that renames, re-IDs, adds, drops or
//! exchanges a table or partition rewrites the label rules naming it
//! (`pkg/ddl/executor.go`, `table.go`, `partition.go`, `schema.go`).
//!
//! The rules live in the store's label manager ([`Catalog::put_label_rule`]
//! and its siblings), outside the catalog's transaction, as Go's live in PD.
//! Rule IDs carry the job's lower-case schema, table and partition names.
//!
//! NOT MODELLED (documented): Go's GC worker deletes a dropped table's rules
//! (`doGCLabelRules`) when it processes the table's delete range. This tier
//! has no GC worker, so those rules stay, hidden by the `ATTRIBUTES` reader's
//! table-existence check exactly as they are in Go before GC runs.

use std::collections::BTreeMap;

use tidb_hack::GoToLower;

use crate::ddl_label::{self, CodecV1, LabelCodec, Rule};
use crate::{Catalog, DriverError, KvTable, TableEntry};

/// Go `getPartitionIDs`.
fn get_partition_ids(table: &KvTable) -> Vec<i64> {
    table.partition().map_or_else(Vec::new, |partition| {
        partition
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect()
    })
}

/// Go `getIDs([]*model.TableInfo{tblInfo})`: the table's ID, then its
/// partitions'.
pub(crate) fn get_ids(table: &KvTable) -> Vec<i64> {
    let mut ids = vec![table.table_id];
    ids.extend(get_partition_ids(table));
    ids
}

/// Go `getPartitionRuleIDs`.
fn get_partition_rule_ids(codec: &dyn LabelCodec, db_name: &str, table: &KvTable) -> Vec<String> {
    let table_name = table.name().go_to_lower();
    table.partition().map_or_else(Vec::new, |partition| {
        partition
            .definitions
            .iter()
            .map(|definition| {
                ddl_label::new_rule_id(codec, db_name, &table_name, &definition.name.go_to_lower())
            })
            .collect()
    })
}

/// What Go `getOldLabelRules` returns: the rule IDs a table and its
/// partitions had under their old names, and the rules stored under them.
pub(crate) struct OldLabelRules {
    table_rule_id: String,
    part_rule_ids: Vec<String>,
    old_rule_ids: Vec<String>,
    old_rules: BTreeMap<String, Rule>,
}

impl OldLabelRules {
    /// Go's `oldRuleIDs`: the table's rule ID, then its partitions'.
    pub(crate) fn old_rule_ids(&self) -> &[String] {
        &self.old_rule_ids
    }
}

/// Go `getOldLabelRules`. `table` supplies the partition definitions, whose
/// order [`update_label_rules`] relies on.
pub(crate) fn get_old_label_rules(
    catalog: &Catalog,
    table: &KvTable,
    old_schema_name: &str,
    old_table_name: &str,
) -> Result<OldLabelRules, DriverError> {
    let codec = CodecV1;
    let table_rule_id = ddl_label::new_rule_id(&codec, old_schema_name, old_table_name, "");
    let part_rule_ids = table.partition().map_or_else(Vec::new, |partition| {
        partition
            .definitions
            .iter()
            .map(|definition| {
                ddl_label::new_rule_id(
                    &codec,
                    old_schema_name,
                    old_table_name,
                    &definition.name.go_to_lower(),
                )
            })
            .collect()
    });
    let mut old_rule_ids = vec![table_rule_id.clone()];
    old_rule_ids.extend(part_rule_ids.iter().cloned());
    let old_rules = catalog.get_label_rules(&old_rule_ids)?;
    Ok(OldLabelRules {
        table_rule_id,
        part_rule_ids,
        old_rule_ids,
        old_rules,
    })
}

/// Go `updateLabelRules`: re-keys the old rules to `table`'s current names
/// and partition IDs under `new_schema_name`, the table itself as `table_id`,
/// and deletes `delete_rule_ids`.
pub(crate) fn update_label_rules(
    catalog: &Catalog,
    new_schema_name: &str,
    table: &KvTable,
    old: &OldLabelRules,
    delete_rule_ids: &[String],
    table_id: i64,
) -> Result<(), DriverError> {
    let codec = CodecV1;
    let table_name = table.name().go_to_lower();
    let mut new_rules = Vec::new();
    if let Some(partition) = table.partition() {
        for (definition, part_rule_id) in partition.definitions.iter().zip(&old.part_rule_ids) {
            if let Some(rule) = old.old_rules.get(part_rule_id) {
                let mut rule = rule.clone_rule();
                rule.reset(
                    &codec,
                    new_schema_name,
                    &table_name,
                    &definition.name.go_to_lower(),
                    &[definition.id],
                );
                new_rules.push(rule);
            }
        }
    }
    if let Some(rule) = old.old_rules.get(&old.table_rule_id) {
        let mut ids = vec![table_id];
        ids.extend(get_partition_ids(table));
        let mut rule = rule.clone_rule();
        rule.reset(&codec, new_schema_name, &table_name, "", &ids);
        new_rules.push(rule);
    }
    catalog.update_label_rules(&ddl_label::new_rule_patch(
        new_rules,
        delete_rule_ids.to_vec(),
    ))
}

/// Go `alterTableLabelRule`: re-ranges the table's rule, when it has one,
/// over `ids`, reporting whether it did.
pub(crate) fn alter_table_label_rule(
    catalog: &Catalog,
    schema_name: &str,
    table: &KvTable,
    ids: &[i64],
) -> Result<bool, DriverError> {
    let codec = CodecV1;
    let table_name = table.name().go_to_lower();
    let table_rule_id = ddl_label::new_rule_id(&codec, schema_name, &table_name, "");
    let old_rule = catalog.get_label_rules(std::slice::from_ref(&table_rule_id))?;
    let Some(rule) = old_rule.get(&table_rule_id) else {
        return Ok(false);
    };
    let mut rule = rule.clone_rule();
    rule.reset(&codec, schema_name, &table_name, "", ids);
    catalog.put_label_rule(&rule)?;
    Ok(true)
}

/// Go `dropLabelRules`: deletes the rules of the named partitions.
pub(crate) fn drop_label_rules(
    catalog: &Catalog,
    schema_name: &str,
    table_name: &str,
    part_names: &[String],
) -> Result<(), DriverError> {
    let codec = CodecV1;
    let delete_rules = part_names
        .iter()
        .map(|part_name| ddl_label::new_rule_id(&codec, schema_name, table_name, part_name))
        .collect();
    catalog.update_label_rules(&ddl_label::new_rule_patch(Vec::new(), delete_rules))
}

/// The label half of Go `updateTruncatePartitionLabelRules` (the placement
/// half delivers bundles this tier does not keep): the table's rule spans
/// every current partition again, and each truncated partition's rule moves
/// to the partition's new ID. `new_partitions` are the truncated partitions'
/// names and new IDs.
pub(crate) fn update_truncate_partition_label_rules(
    catalog: &Catalog,
    schema_name: &str,
    table: &KvTable,
    new_partitions: &[(String, i64)],
) -> Result<(), DriverError> {
    let codec = CodecV1;
    let table_name = table.name().go_to_lower();
    let table_rule_id = ddl_label::new_rule_id(&codec, schema_name, &table_name, "");
    let old_part_rules = new_partitions
        .iter()
        .map(|(name, _)| {
            ddl_label::new_rule_id(&codec, schema_name, &table_name, &name.go_to_lower())
        })
        .collect::<Vec<_>>();
    let mut rule_ids = old_part_rules.clone();
    rule_ids.push(table_rule_id.clone());
    let rules = catalog.get_label_rules(&rule_ids)?;
    let mut new_rules = Vec::with_capacity(new_partitions.len() + 1);
    if let Some(rule) = rules.get(&table_rule_id) {
        let mut ids = get_partition_ids(table);
        ids.push(table.table_id);
        let mut rule = rule.clone_rule();
        rule.reset(&codec, schema_name, &table_name, "", &ids);
        new_rules.push(rule);
    }
    for ((name, id), rule_id) in new_partitions.iter().zip(&old_part_rules) {
        if let Some(rule) = rules.get(rule_id) {
            let mut rule = rule.clone_rule();
            rule.reset(
                &codec,
                schema_name,
                &table_name,
                &name.go_to_lower(),
                &[*id],
            );
            new_rules.push(rule);
        }
    }
    catalog.update_label_rules(&ddl_label::new_rule_patch(new_rules, Vec::new()))
}

/// Which of Go's partition jobs a statement ran, for its label-rule step.
#[derive(Clone, Copy)]
pub(super) enum PartitionLabelJob {
    /// `onAddTablePartition` and `onReorganizePartition` (REORGANIZE,
    /// COALESCE, HASH/KEY ADD, PARTITION BY, REMOVE PARTITIONING): the
    /// table's rule spans the table, the partitions it had, and the ones the
    /// job added (Go's `getIDs(tblInfo)` plus `AddingDefinitions`).
    AddOrReorganize,
    /// `onDropTablePartition`: the dropped partitions' rules go, and the
    /// table's rule spans what remains.
    Drop,
    /// `onTruncateTablePartition`: `updateTruncatePartitionLabelRules`.
    Truncate,
}

/// Runs a partition job, then the label-rule step Go's job worker takes for
/// it, derived from the table's partitions before and after: the IDs it
/// added, dropped or replaced. A job that changed nothing (an `IF EXISTS` or
/// `IF NOT EXISTS` with nothing to do) submits none in Go, so takes no step.
pub(super) fn with_partition_label_rules(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    job: PartitionLabelJob,
    run: impl FnOnce(&mut Catalog) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    let partitions_of = |table: &KvTable| {
        table.partition().map_or_else(Vec::new, |partition| {
            partition
                .definitions
                .iter()
                .map(|definition| (definition.name.go_to_lower(), definition.id))
                .collect::<Vec<_>>()
        })
    };
    let before = match catalog.table_in(database, table_name) {
        Some(TableEntry::Kv(table)) => (get_ids(table), partitions_of(table)),
        _ => return run(catalog),
    };
    run(catalog)?;
    let Some(TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Ok(());
    };
    let after_ids = get_ids(table);
    if after_ids == before.0 && partitions_of(table) == before.1 {
        return Ok(());
    }
    let schema_name = database.go_to_lower();
    match job {
        PartitionLabelJob::AddOrReorganize => {
            let mut ids = before.0;
            for id in after_ids {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            alter_table_label_rule(catalog, &schema_name, table, &ids)?;
        }
        PartitionLabelJob::Drop => {
            let dropped = before
                .1
                .iter()
                .filter(|(_, id)| !after_ids.contains(id))
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            drop_label_rules(catalog, &schema_name, &table.name().go_to_lower(), &dropped)?;
            alter_table_label_rule(catalog, &schema_name, table, &after_ids)?;
        }
        PartitionLabelJob::Truncate => {
            let replaced = partitions_of(table)
                .into_iter()
                .filter(|partition| !before.1.contains(partition))
                .collect::<Vec<_>>();
            update_truncate_partition_label_rules(catalog, &schema_name, table, &replaced)?;
        }
    }
    Ok(())
}

/// Go `onExchangeTablePartition`'s label step, run once the physical IDs
/// have swapped: the standalone table's rule moves to the partition and the
/// partition's to the table.
pub(super) fn exchanged_partition_label_rules(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    partition: &str,
    standalone: &[String],
    current_db: &str,
) -> Result<(), DriverError> {
    let (standalone_db, standalone_name) =
        crate::driver::split_table_path_pub(standalone, current_db)?;
    let (Some(TableEntry::Kv(partitioned)), Some(TableEntry::Kv(standalone_table))) = (
        catalog.table_in(database, table_name),
        catalog.table_in(standalone_db, standalone_name),
    ) else {
        return Ok(());
    };
    let partition_name = partition.go_to_lower();
    let Some(partition_id) = partitioned.partition().and_then(|partitioning| {
        partitioning
            .definitions
            .iter()
            .find(|definition| definition.name.go_to_lower() == partition_name)
            .map(|definition| definition.id)
    }) else {
        return Ok(());
    };
    ExchangePartitionLabelSwap {
        partitioned_schema: database.go_to_lower(),
        partitioned_table: partitioned.name().go_to_lower(),
        partition: partition_name,
        standalone_schema: standalone_db.go_to_lower(),
        standalone_table: standalone_table.name().go_to_lower(),
        partition_id,
        standalone_id: standalone_table.table_id,
    }
    .deliver(catalog)
}

/// The rule IDs Go's `onDropSchema` deletes when the schema leaves the
/// public state: each table's partition rules, then its own.
pub fn drop_schema_label_rules(catalog: &Catalog, schema_name: &str) -> Result<(), DriverError> {
    let codec = CodecV1;
    let schema_name = schema_name.go_to_lower();
    let mut rule_ids = Vec::new();
    for table_name in catalog.table_names(&schema_name).unwrap_or_default() {
        if let Some(TableEntry::Kv(table)) = catalog.persistent_table_in(&schema_name, &table_name)
        {
            rule_ids.extend(get_partition_rule_ids(&codec, &schema_name, table));
        }
        rule_ids.push(ddl_label::new_rule_id(
            &codec,
            &schema_name,
            &table_name.go_to_lower(),
            "",
        ));
    }
    catalog.update_label_rules(&ddl_label::new_rule_patch(Vec::new(), rule_ids))
}

/// Go's `ErrInvalidAttributesSpec` (8237).
fn invalid_attributes_spec(error: &ddl_label::LabelError) -> DriverError {
    DriverError::DdlCoded {
        errno: 8237,
        message: format!("Invalid attributes: {error}"),
    }
}

/// Go `onAlterTableAttributes` / `onAlterTablePartitionAttributes`: a rule
/// with no labels (`ATTRIBUTES=DEFAULT`, or an empty list) deletes the rule
/// ID; any other replaces the stored rule.
fn deliver_attributes_rule(catalog: &Catalog, rule: &Rule) -> Result<(), DriverError> {
    if rule.labels.with_visible(<[_]>::is_empty) {
        return catalog.update_label_rules(&ddl_label::new_rule_patch(
            Vec::new(),
            vec![rule.id.clone()],
        ));
    }
    catalog.put_label_rule(rule)
}

/// Go `executor.AlterTableAttributes` and its job, `onAlterTableAttributes`:
/// the rule spans the table and every partition.
pub(super) fn alter_table_attributes_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    spec: &tidb_ast::AttributesSpec,
) -> Result<(), DriverError> {
    let mut rule = Rule::new();
    rule.apply_attributes_spec(spec)
        .map_err(|error| invalid_attributes_spec(&error))?;
    let Some(TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let ids = get_ids(table);
    rule.reset(
        &CodecV1,
        &database.go_to_lower(),
        &table.name().go_to_lower(),
        "",
        &ids,
    );
    deliver_attributes_rule(catalog, &rule)?;
    // Go `updateVersionAndTableInfo(..., true)`: the job moves the schema
    // version.
    catalog.table_mut_in(database, table_name);
    Ok(())
}

/// Go `executor.AlterTablePartitionAttributes` and its job,
/// `onAlterTablePartitionAttributes`: the rule spans the one partition.
pub(super) fn alter_table_partition_attributes_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    partition_name: &str,
    attributes: &Option<String>,
) -> Result<(), DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let Some(partition) = table.partition() else {
        return Err(DriverError::PartitionManagementOnNonpartitioned);
    };
    // Go `tables.FindPartitionByName(meta, spec.PartitionNames[0].L)`.
    let partition_name = partition_name.go_to_lower();
    let Some(partition_id) = partition
        .definitions
        .iter()
        .find(|definition| definition.name.go_to_lower() == partition_name)
        .map(|definition| definition.id)
    else {
        return Err(DriverError::UnknownPartition {
            partition: partition_name,
            table: table.name().to_owned(),
        });
    };
    let mut rule = Rule::new();
    rule.apply_attributes_spec(&tidb_ast::AttributesSpec {
        attributes: attributes.clone(),
    })
    .map_err(|error| invalid_attributes_spec(&error))?;
    rule.reset(
        &CodecV1,
        &database.go_to_lower(),
        &table.name().go_to_lower(),
        &partition_name,
        &[partition_id],
    );
    deliver_attributes_rule(catalog, &rule)?;
    catalog.table_mut_in(database, table_name);
    Ok(())
}

/// The names and post-exchange physical IDs Go uses to swap the standalone
/// table and partition label rules.
#[derive(Clone, Debug)]
pub struct ExchangePartitionLabelSwap {
    /// Partitioned-table database name.
    pub partitioned_schema: String,
    /// Partitioned-table name.
    pub partitioned_table: String,
    /// Exchanged partition name.
    pub partition: String,
    /// Standalone-table database name.
    pub standalone_schema: String,
    /// Standalone-table name.
    pub standalone_table: String,
    /// Physical ID now owned by the named partition.
    pub partition_id: i64,
    /// Physical ID now owned by the standalone table.
    pub standalone_id: i64,
}

impl ExchangePartitionLabelSwap {
    /// The two existing rule IDs Go fetches before constructing its patch.
    pub fn rule_ids(&self, codec: &dyn LabelCodec) -> [String; 2] {
        [
            ddl_label::new_rule_id(codec, &self.standalone_schema, &self.standalone_table, ""),
            ddl_label::new_rule_id(
                codec,
                &self.partitioned_schema,
                &self.partitioned_table,
                &self.partition,
            ),
        ]
    }

    /// Pinned Go `onExchangeTablePartition`'s four-way label-rule patch.
    pub fn patch(&self, codec: &dyn LabelCodec, rules: &[Rule]) -> ddl_label::LabelRulePatch {
        let [standalone_rule_id, partition_rule_id] = self.rule_ids(codec);
        let standalone_rule = rules.iter().find(|rule| rule.id == standalone_rule_id);
        let partition_rule = rules.iter().find(|rule| rule.id == partition_rule_id);
        let mut set_rules = Vec::with_capacity(2);
        let mut delete_rules = Vec::with_capacity(1);
        if let Some(rule) = standalone_rule {
            let mut rule = rule.clone_rule();
            rule.reset(
                codec,
                &self.partitioned_schema,
                &self.partitioned_table,
                &self.partition,
                &[self.partition_id],
            );
            set_rules.push(rule);
            if partition_rule.is_none() {
                delete_rules.push(standalone_rule_id);
            }
        }
        if let Some(rule) = partition_rule {
            let mut rule = rule.clone_rule();
            rule.reset(
                codec,
                &self.standalone_schema,
                &self.standalone_table,
                "",
                &[self.standalone_id],
            );
            set_rules.push(rule);
            if standalone_rule.is_none() {
                delete_rules.push(partition_rule_id);
            }
        }
        ddl_label::new_rule_patch(set_rules, delete_rules)
    }

    /// Go `onExchangeTablePartition`'s label step against this store's label
    /// manager.
    pub(crate) fn deliver(&self, catalog: &Catalog) -> Result<(), DriverError> {
        let codec = CodecV1;
        let rules = catalog
            .get_label_rules(&self.rule_ids(&codec))?
            .into_values()
            .collect::<Vec<_>>();
        catalog.update_label_rules(&self.patch(&codec, &rules))
    }
}
