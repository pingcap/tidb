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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go physicalop/foreign_key.go: one resolved policy for execution and EXPLAIN.

use crate::driver::catalog::{Catalog, TableEntry};
use crate::driver::DriverError;
use crate::kv_table::{FkAction, KvForeignKey};
use crate::StmtContext;
use tidb_planner::physical::{FkTriggerKind, FkTriggerNode};
use tidb_planner::plan_base::PlanIdAllocator;

#[derive(Clone, Debug, Default)]
pub(crate) struct FkPlanSpec {
    /// The affected table's database (Go `tnW.DBInfo.Name.L`).
    pub database: String,
    /// The affected table name.
    pub table: String,
    /// Go `buildTbl2UpdateColumns` for UPDATE: the SET column names.
    pub updated_cols: Vec<String>,
    /// Go `Insert.IsReplace`.
    pub replace: bool,
    /// Go `buildOnDuplicateUpdateColumns`: the ON DUPLICATE KEY UPDATE
    /// assignment targets.
    pub on_duplicate_cols: Vec<String>,
}

/// Select constraints once, following Go's BuildOn{Insert,Update,Delete}FKTriggers.
pub(crate) fn build_fk_triggers(
    catalog: &Catalog,
    ctx: &StmtContext,
    operator: &str,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Result<Vec<FkTriggerNode>, DriverError> {
    if !ctx.foreign_key_checks() {
        return Ok(Vec::new());
    }
    // Go buildUpdateLists appends generated assignments when their
    // dependencies may change, including transitive generated dependencies
    // and ON UPDATE timestamps. ODKU uses the same completed update columns.
    let mut spec = spec.clone();
    if let Some(TableEntry::Kv(table)) = catalog.get_in(&spec.database, &spec.table) {
        for updated in [&mut spec.updated_cols, &mut spec.on_duplicate_cols] {
            if updated.is_empty() {
                continue;
            }
            let mut modified = updated.clone();
            modified.extend(
                table
                    .columns
                    .iter()
                    .filter(|column| {
                        column
                            .field_type
                            .has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW)
                    })
                    .map(|column| column.name.clone()),
            );
            for column in table.columns.iter() {
                if column
                    .generated
                    .as_ref()
                    .is_some_and(|generation| touches(&modified, &generation.dependencies))
                {
                    if !modified
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&column.name))
                    {
                        modified.push(column.name.clone());
                    }
                    if !updated
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&column.name))
                    {
                        updated.push(column.name.clone());
                    }
                }
            }
        }
    }
    let mut nodes = Vec::new();
    let parent_change = match operator {
        "Delete" => Some((true, &[][..])),
        "Update" if !spec.updated_cols.is_empty() => Some((false, spec.updated_cols.as_slice())),
        "Insert" if !spec.on_duplicate_cols.is_empty() => {
            Some((false, spec.on_duplicate_cols.as_slice()))
        }
        "Insert" if spec.replace => Some((true, &[][..])),
        _ => None,
    };
    if let Some((on_delete, updated)) = parent_change {
        for (child_db, child_table, fk) in
            crate::foreign_key::referring(catalog, &spec.database, &spec.table)
        {
            if !on_delete && !touches(updated, &fk.ref_cols) {
                continue;
            }
            let action = if on_delete {
                fk.on_delete
            } else {
                fk.on_update
            };
            let kind = match action {
                FkAction::Cascade | FkAction::SetNull => FkTriggerKind::Cascade {
                    on_delete,
                    set_null: action == FkAction::SetNull,
                },
                _ => FkTriggerKind::ParentCheck,
            };
            if let Some(node) = compile_trigger(
                catalog,
                &child_db,
                &child_table,
                &fk,
                kind,
                action,
                plan_ids,
            )? {
                nodes.push(node);
            }
        }
    }
    if let Some(TableEntry::Kv(table)) = catalog.get_in(&spec.database, &spec.table) {
        for fk in table.foreign_keys() {
            if operator != "Insert"
                && (operator != "Update" || !touches(&spec.updated_cols, &fk.cols))
            {
                continue;
            }
            if let Some(node) = compile_trigger(
                catalog,
                &spec.database,
                &spec.table,
                fk,
                FkTriggerKind::ChildCheck,
                FkAction::NoOption,
                plan_ids,
            )? {
                nodes.push(node);
            }
        }
    }
    Ok(nodes)
}

fn touches(updated: &[String], columns: &[String]) -> bool {
    columns
        .iter()
        .any(|column| updated.iter().any(|name| name.eq_ignore_ascii_case(column)))
}

fn compile_trigger(
    catalog: &Catalog,
    child_db: &str,
    child_table: &str,
    fk: &KvForeignKey,
    kind: FkTriggerKind,
    action: FkAction,
    plan_ids: &PlanIdAllocator,
) -> Result<Option<FkTriggerNode>, DriverError> {
    let Some(TableEntry::Kv(parent)) = catalog.get_in(&fk.ref_schema, &fk.ref_table) else {
        // Go buildFKCheckOnModifyChildTable omits a missing parent.
        return Ok(None);
    };
    let Some(TableEntry::Kv(child)) = catalog.get_in(child_db, child_table) else {
        return Ok(None);
    };
    let offsets = |table: &crate::kv_table::KvTable,
                   names: &[String]|
     -> Result<Vec<usize>, DriverError> {
        names
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        DriverError::unsupported(format!("foreign key column {name} is not found"))
                    })
            })
            .collect()
    };
    let child_offsets = offsets(child, &fk.cols)?;
    let parent_offsets = offsets(parent, &fk.ref_cols)?;
    let constraint = crate::foreign_key::constraint_text(fk);
    let failure = || {
        let table = format!("`{child_db}`.`{child_table}`");
        if kind == FkTriggerKind::ChildCheck {
            DriverError::ForeignKeyNoReferencedRow {
                table,
                constraint: constraint.clone(),
            }
        } else {
            DriverError::ForeignKeyRowIsReferenced {
                table,
                constraint: constraint.clone(),
            }
        }
    };
    let (table, name, columns) = if kind == FkTriggerKind::ChildCheck {
        (parent, fk.ref_table.as_str(), &parent_offsets)
    } else {
        (child, child_table, &child_offsets)
    };
    let mut access = format!("table:{name}");
    let Some(lookup_index) = table.foreign_key_lookup_index(columns) else {
        return Err(if matches!(kind, FkTriggerKind::Cascade { .. }) {
            DriverError::unsupported(format!(
                "Missing index for '{}' foreign key columns in the table '{}'",
                fk.name, child_table
            ))
        } else {
            failure()
        });
    };
    if let Some(id) = lookup_index {
        let index = table.indexes().iter().find(|index| index.id == id).unwrap();
        access.push_str(&format!(", index:{}", index.name));
    }
    let (operator, info) = match kind {
        FkTriggerKind::ChildCheck => (
            "Foreign_Key_Check",
            format!("foreign_key:{}, check_exist", fk.name),
        ),
        FkTriggerKind::ParentCheck => (
            "Foreign_Key_Check",
            format!("foreign_key:{}, check_not_exist", fk.name),
        ),
        FkTriggerKind::Cascade { on_delete, .. } => (
            "Foreign_Key_Cascade",
            format!(
                "foreign_key:{}, {}:{}",
                fk.name,
                if on_delete { "on_delete" } else { "on_update" },
                if action == FkAction::SetNull {
                    "SET NULL"
                } else {
                    "CASCADE"
                }
            ),
        ),
    };
    Ok(Some(FkTriggerNode {
        id: plan_ids.alloc(),
        operator,
        access,
        info,
        kind,
        child_database: child_db.to_owned(),
        child_table: child_table.to_owned(),
        parent_database: fk.ref_schema.clone(),
        parent_table: fk.ref_table.clone(),
        child_offsets,
        parent_offsets,
        lookup_index,
        constraint,
    }))
}
