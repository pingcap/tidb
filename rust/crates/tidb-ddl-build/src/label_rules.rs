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
//! The helpers here are shared by both engines: each reads the store's label
//! manager ([`LabelRuleManager`]) where Go's does and returns the write Go's
//! makes, as a [`LabelRuleWrite`]. The in-process engine applies it at once
//! to its catalog's manager; the production node delivers it to the node's
//! manager (PD, or the node's mock) before the job's transaction commits.
//! Rule IDs carry the job's lower-case schema, table and partition names.
//!
//! NOT MODELLED (documented): Go's GC worker deletes a dropped table's rules
//! (`doGCLabelRules`) when it processes the table's delete range. Neither
//! engine has that step, so those rules stay, hidden by the `ATTRIBUTES`
//! reader's table-existence check exactly as they are in Go before GC runs.

use std::collections::BTreeMap;

use tidb_hack::GoToLower;

use tidb_ddl_label::{self as ddl_label, CodecV1, LabelCodec, LabelRulePatch, Rule};
use tidb_domain_infosync::label_manager::LabelRuleManager;
use tidb_table::kv_table::KvTable;

/// What Go's label-rule helpers read of a `TableInfo`: its lower-case name,
/// its ID, and its partitions' lower-case names and IDs in definition order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelRuleTable {
    /// Go `tblInfo.Name.L`.
    pub name: String,
    /// Go `tblInfo.ID`.
    pub id: i64,
    /// Go `tblInfo.Partition.Definitions`' `Name.L` and `ID`.
    pub partitions: Vec<(String, i64)>,
}

impl LabelRuleTable {
    /// The in-process table's names and IDs.
    #[must_use]
    pub fn from_kv(table: &KvTable) -> Self {
        Self {
            name: table.name().go_to_lower(),
            id: table.table_id(),
            partitions: table.partition().map_or_else(Vec::new, |partition| {
                partition
                    .definitions
                    .iter()
                    .map(|definition| (definition.name.go_to_lower(), definition.id))
                    .collect()
            }),
        }
    }

    /// A stored `TableInfo`'s names and IDs.
    #[must_use]
    pub fn from_table_info(table: &tidb_model::TableInfo) -> Self {
        Self {
            name: table.name.lowercase().to_owned(),
            id: table.id,
            partitions: table
                .get_partition_info()
                .map(|partition| {
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .iter()
                        .map(|definition| (definition.name.lowercase().to_owned(), definition.id))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    /// Go `getPartitionIDs`.
    fn partition_ids(&self) -> Vec<i64> {
        self.partitions.iter().map(|(_, id)| *id).collect()
    }

    /// Go `getIDs([]*model.TableInfo{tblInfo})`: the table's ID, then its
    /// partitions'.
    #[must_use]
    pub fn ids(&self) -> Vec<i64> {
        let mut ids = vec![self.id];
        ids.extend(self.partition_ids());
        ids
    }

    /// Go `getPartitionRuleIDs`.
    fn partition_rule_ids(&self, codec: &dyn LabelCodec, schema_name: &str) -> Vec<String> {
        self.partitions
            .iter()
            .map(|(name, _)| ddl_label::new_rule_id(codec, schema_name, &self.name, name))
            .collect()
    }
}

/// One of Go's label-rule calls from a DDL job step.
#[derive(Clone, Debug)]
pub enum LabelRuleWrite {
    /// Go `infosync.PutLabelRule`.
    Put(Rule),
    /// Go `infosync.UpdateLabelRules`.
    Patch(LabelRulePatch),
}

impl LabelRuleWrite {
    /// Makes the call against `manager`. An empty patch is no call, as Go's
    /// `infosync.UpdateLabelRules` returns before reaching the manager.
    pub fn apply(&self, manager: &dyn LabelRuleManager) -> Result<(), String> {
        match self {
            Self::Put(rule) => manager.put_label_rule(rule),
            Self::Patch(patch) => {
                if patch.set_rules.with_visible(<[_]>::is_empty)
                    && patch.delete_rules.with_visible(<[_]>::is_empty)
                {
                    return Ok(());
                }
                manager.update_label_rules(patch)
            }
        }
    }

    /// Every rule ID the call sets or deletes.
    #[must_use]
    pub fn rule_ids(&self) -> Vec<String> {
        match self {
            Self::Put(rule) => vec![rule.id.clone()],
            Self::Patch(patch) => {
                let mut ids = patch.delete_rules.snapshot();
                ids.extend(patch.set_rules.snapshot().into_iter().map(|rule| rule.id));
                ids
            }
        }
    }
}

/// What Go `getOldLabelRules` returns: the rule IDs a table and its
/// partitions had under their old names, and the rules stored under them.
pub struct OldLabelRules {
    table_rule_id: String,
    part_rule_ids: Vec<String>,
    old_rule_ids: Vec<String>,
    old_rules: BTreeMap<String, Rule>,
}

impl OldLabelRules {
    /// Go's `oldRuleIDs`: the table's rule ID, then its partitions'.
    #[must_use]
    pub fn old_rule_ids(&self) -> &[String] {
        &self.old_rule_ids
    }
}

/// Go `getOldLabelRules`. `table` supplies the partition definitions, whose
/// order [`update_label_rules`] relies on.
pub fn get_old_label_rules(
    manager: &dyn LabelRuleManager,
    table: &LabelRuleTable,
    old_schema_name: &str,
    old_table_name: &str,
) -> Result<OldLabelRules, String> {
    let codec = CodecV1;
    let table_rule_id = ddl_label::new_rule_id(&codec, old_schema_name, old_table_name, "");
    let part_rule_ids = table
        .partitions
        .iter()
        .map(|(name, _)| ddl_label::new_rule_id(&codec, old_schema_name, old_table_name, name))
        .collect::<Vec<_>>();
    let mut old_rule_ids = vec![table_rule_id.clone()];
    old_rule_ids.extend(part_rule_ids.iter().cloned());
    let old_rules = manager.get_label_rules(&old_rule_ids)?;
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
#[must_use]
pub fn update_label_rules(
    new_schema_name: &str,
    table: &LabelRuleTable,
    old: &OldLabelRules,
    delete_rule_ids: &[String],
    table_id: i64,
) -> LabelRuleWrite {
    let codec = CodecV1;
    let mut new_rules = Vec::new();
    for ((name, id), part_rule_id) in table.partitions.iter().zip(&old.part_rule_ids) {
        if let Some(rule) = old.old_rules.get(part_rule_id) {
            let mut rule = rule.clone_rule();
            rule.reset(&codec, new_schema_name, &table.name, name, &[*id]);
            new_rules.push(rule);
        }
    }
    if let Some(rule) = old.old_rules.get(&old.table_rule_id) {
        let mut ids = vec![table_id];
        ids.extend(table.partition_ids());
        let mut rule = rule.clone_rule();
        rule.reset(&codec, new_schema_name, &table.name, "", &ids);
        new_rules.push(rule);
    }
    LabelRuleWrite::Patch(ddl_label::new_rule_patch(
        new_rules,
        delete_rule_ids.to_vec(),
    ))
}

/// Go `alterTableLabelRule`: re-ranges the table's rule, when it has one,
/// over `ids`.
pub fn alter_table_label_rule(
    manager: &dyn LabelRuleManager,
    schema_name: &str,
    table: &LabelRuleTable,
    ids: &[i64],
) -> Result<Option<LabelRuleWrite>, String> {
    let codec = CodecV1;
    let table_rule_id = ddl_label::new_rule_id(&codec, schema_name, &table.name, "");
    let old_rule = manager.get_label_rules(std::slice::from_ref(&table_rule_id))?;
    let Some(rule) = old_rule.get(&table_rule_id) else {
        return Ok(None);
    };
    let mut rule = rule.clone_rule();
    rule.reset(&codec, schema_name, &table.name, "", ids);
    Ok(Some(LabelRuleWrite::Put(rule)))
}

/// Go `dropLabelRules`: deletes the rules of the named partitions.
#[must_use]
pub fn drop_label_rules(
    schema_name: &str,
    table_name: &str,
    part_names: &[String],
) -> LabelRuleWrite {
    let codec = CodecV1;
    let delete_rules = part_names
        .iter()
        .map(|part_name| ddl_label::new_rule_id(&codec, schema_name, table_name, part_name))
        .collect();
    LabelRuleWrite::Patch(ddl_label::new_rule_patch(Vec::new(), delete_rules))
}

/// The label half of Go `updateTruncatePartitionLabelRules` (the placement
/// half delivers bundles): the table's rule spans every current partition
/// again, and each truncated partition's rule moves to the partition's new
/// ID. `new_partitions` are the truncated partitions' lower-case names and
/// new IDs.
pub fn update_truncate_partition_label_rules(
    manager: &dyn LabelRuleManager,
    schema_name: &str,
    table: &LabelRuleTable,
    new_partitions: &[(String, i64)],
) -> Result<LabelRuleWrite, String> {
    let codec = CodecV1;
    let table_rule_id = ddl_label::new_rule_id(&codec, schema_name, &table.name, "");
    let old_part_rules = new_partitions
        .iter()
        .map(|(name, _)| ddl_label::new_rule_id(&codec, schema_name, &table.name, name))
        .collect::<Vec<_>>();
    let mut rule_ids = old_part_rules.clone();
    rule_ids.push(table_rule_id.clone());
    let rules = manager.get_label_rules(&rule_ids)?;
    let mut new_rules = Vec::with_capacity(new_partitions.len() + 1);
    if let Some(rule) = rules.get(&table_rule_id) {
        let mut ids = table.partition_ids();
        ids.push(table.id);
        let mut rule = rule.clone_rule();
        rule.reset(&codec, schema_name, &table.name, "", &ids);
        new_rules.push(rule);
    }
    for ((name, id), rule_id) in new_partitions.iter().zip(&old_part_rules) {
        if let Some(rule) = rules.get(rule_id) {
            let mut rule = rule.clone_rule();
            rule.reset(&codec, schema_name, &table.name, name, &[*id]);
            new_rules.push(rule);
        }
    }
    Ok(LabelRuleWrite::Patch(ddl_label::new_rule_patch(
        new_rules,
        Vec::new(),
    )))
}

/// Which of Go's partition jobs a statement ran, for its label-rule step.
#[derive(Clone, Copy, Debug)]
pub enum PartitionLabelJob {
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

/// The label-rule step Go's job worker takes for a partition job, derived
/// from the table's partitions before and after it: the IDs it added,
/// dropped or replaced. A job that changed nothing (an `IF EXISTS` or
/// `IF NOT EXISTS` with nothing to do) submits none in Go, so takes no step.
pub fn partition_label_writes(
    manager: &dyn LabelRuleManager,
    schema_name: &str,
    before: &LabelRuleTable,
    after: &LabelRuleTable,
    job: PartitionLabelJob,
) -> Result<Vec<LabelRuleWrite>, String> {
    let after_ids = after.ids();
    let before_ids = before.ids();
    if after_ids == before_ids && after.partitions == before.partitions {
        return Ok(Vec::new());
    }
    let mut writes = Vec::new();
    match job {
        PartitionLabelJob::AddOrReorganize => {
            let mut ids = before_ids;
            for id in after_ids {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            writes.extend(alter_table_label_rule(manager, schema_name, after, &ids)?);
        }
        PartitionLabelJob::Drop => {
            let dropped = before
                .partitions
                .iter()
                .filter(|(_, id)| !after_ids.contains(id))
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            writes.push(drop_label_rules(schema_name, &after.name, &dropped));
            writes.extend(alter_table_label_rule(
                manager,
                schema_name,
                after,
                &after_ids,
            )?);
        }
        PartitionLabelJob::Truncate => {
            let replaced = after
                .partitions
                .iter()
                .filter(|partition| !before.partitions.contains(partition))
                .cloned()
                .collect::<Vec<_>>();
            writes.push(update_truncate_partition_label_rules(
                manager,
                schema_name,
                after,
                &replaced,
            )?);
        }
    }
    Ok(writes)
}

/// The rule IDs Go's `onDropSchema` deletes when the schema leaves the
/// public state: each table's partition rules, then its own.
#[must_use]
pub fn drop_schema_label_rules(schema_name: &str, tables: &[LabelRuleTable]) -> LabelRuleWrite {
    let codec = CodecV1;
    let mut rule_ids = Vec::new();
    for table in tables {
        rule_ids.extend(table.partition_rule_ids(&codec, schema_name));
        rule_ids.push(ddl_label::new_rule_id(&codec, schema_name, &table.name, ""));
    }
    LabelRuleWrite::Patch(ddl_label::new_rule_patch(Vec::new(), rule_ids))
}

/// Go's `ErrInvalidAttributesSpec` (8237) message.
#[must_use]
pub fn invalid_attributes_spec_message(error: &ddl_label::LabelError) -> String {
    format!("Invalid attributes: {error}")
}

/// Go `executor.AlterTableAttributes`' rule: the spec's labels over the table
/// and every partition.
pub fn table_attributes_rule(
    spec: &tidb_ast::AttributesSpec,
    schema_name: &str,
    table: &LabelRuleTable,
) -> Result<Rule, ddl_label::LabelError> {
    let mut rule = Rule::new();
    rule.apply_attributes_spec(spec)?;
    rule.reset(&CodecV1, schema_name, &table.name, "", &table.ids());
    Ok(rule)
}

/// Go `executor.AlterTablePartitionAttributes`' rule: the spec's labels over
/// the one partition.
pub fn partition_attributes_rule(
    attributes: &Option<String>,
    schema_name: &str,
    table_name: &str,
    partition_name: &str,
    partition_id: i64,
) -> Result<Rule, ddl_label::LabelError> {
    let mut rule = Rule::new();
    rule.apply_attributes_spec(&tidb_ast::AttributesSpec {
        attributes: attributes.clone(),
    })?;
    rule.reset(
        &CodecV1,
        schema_name,
        table_name,
        partition_name,
        &[partition_id],
    );
    Ok(rule)
}

/// Go `onAlterTableAttributes` / `onAlterTablePartitionAttributes`: a rule
/// with no labels (`ATTRIBUTES=DEFAULT`, or an empty list) deletes the rule
/// ID; any other replaces the stored rule.
#[must_use]
pub fn attributes_rule_write(rule: Rule) -> LabelRuleWrite {
    if rule.labels.with_visible(<[_]>::is_empty) {
        return LabelRuleWrite::Patch(ddl_label::new_rule_patch(Vec::new(), vec![rule.id]));
    }
    LabelRuleWrite::Put(rule)
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

    /// Go `onExchangeTablePartition`'s label step: the patch over the rules
    /// `manager` holds under the two IDs.
    pub fn label_write(&self, manager: &dyn LabelRuleManager) -> Result<LabelRuleWrite, String> {
        let codec = CodecV1;
        let rules = manager
            .get_label_rules(&self.rule_ids(&codec))?
            .into_values()
            .collect::<Vec<_>>();
        Ok(LabelRuleWrite::Patch(self.patch(&codec, &rules)))
    }
}
