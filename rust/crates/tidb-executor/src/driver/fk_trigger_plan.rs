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

//! Go `pkg/planner/core/operator/physicalop/foreign_key.go`: the FKCheck /
//! FKCascade EXPLAIN leaves `BuildOn{Insert,Update,Delete}FKTriggers` attach
//! to the INSERT/UPDATE/DELETE plan.
//!
//! The write path enforces the same constraints at runtime through
//! `crate::foreign_key` (`FKCheckExec` and the cascade operators); these
//! nodes exist so `EXPLAIN` renders the exact plan tree Go renders, with the
//! plan ids Go allocates when the trigger builders run.

use crate::driver::catalog::{Catalog, TableEntry};
use crate::StmtContext;
use tidb_planner::physical::FkTriggerNode;
use tidb_planner::plan_base::PlanIdAllocator;

use crate::kv_table::table_meta::KvForeignKey;

const FK_CHECK: &str = "Foreign_Key_Check";
const FK_CASCADE: &str = "Foreign_Key_Cascade";

/// What the statement-level builders need to know about the DML target,
/// gathered from the AST by the caller (`explain_insert_stmt` and friends).
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

/// Go `BuildOnInsertFKTriggers` / `BuildOnUpdateFKTriggers` /
/// `BuildOnDeleteFKTriggers`, dispatched by the DML operator. Allocates one
/// plan id per node exactly where Go's `Init` does.
pub(crate) fn build_fk_triggers(
    catalog: &Catalog,
    ctx: &StmtContext,
    operator: &str,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Vec<FkTriggerNode> {
    // Go: `if !ctx.GetSessionVars().ForeignKeyChecks { return nil }`.
    if !ctx.foreign_key_checks() {
        return Vec::new();
    }
    match operator {
        "Insert" => build_on_insert_fk_triggers(catalog, spec, plan_ids),
        "Update" => build_on_update_fk_triggers(catalog, spec, plan_ids),
        "Delete" => build_on_delete_fk_triggers(catalog, spec, plan_ids),
        _ => Vec::new(),
    }
}

fn declared_foreign_keys<'a>(
    catalog: &'a Catalog,
    database: &str,
    table: &str,
) -> Vec<KvForeignKey> {
    match catalog.get_in(database, table) {
        Some(TableEntry::Kv(kv)) => kv.foreign_keys().to_vec(),
        _ => Vec::new(),
    }
}

/// Go `BuildOnInsertFKTriggers`: the ON DUPLICATE / REPLACE referred-side
/// triggers first, then one `check_exist` per declared foreign key.
fn build_on_insert_fk_triggers(
    catalog: &Catalog,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Vec<FkTriggerNode> {
    let mut nodes = Vec::new();
    if !spec.on_duplicate_cols.is_empty() {
        // Go `buildOnUpdateReferredFKTriggers` over the assignment targets.
        nodes.extend(build_referred_fk_triggers(
            catalog,
            &spec.database,
            &spec.table,
            &spec.on_duplicate_cols,
            FkTriggerType::OnUpdate,
            plan_ids,
        ));
    } else if spec.replace {
        // Go `buildOnReplaceReferredFKTriggers`: every referred FK, delete form.
        nodes.extend(build_referred_fk_triggers(
            catalog,
            &spec.database,
            &spec.table,
            &[],
            FkTriggerType::OnDelete,
            plan_ids,
        ));
    }
    for fk in declared_foreign_keys(catalog, &spec.database, &spec.table) {
        if let Some(node) = build_fk_check_on_modify_child_table(
            catalog,
            &spec.database,
            &spec.table,
            &fk,
            plan_ids,
        ) {
            nodes.push(node);
        }
    }
    nodes
}

/// Go `BuildOnUpdateFKTriggers` for the single modified table: referred-side
/// triggers whose parent columns the SET list touches, then child-side
/// checks whose FK columns the SET list touches.
fn build_on_update_fk_triggers(
    catalog: &Catalog,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Vec<FkTriggerNode> {
    if spec.updated_cols.is_empty() {
        return Vec::new();
    }
    let mut nodes = build_referred_fk_triggers(
        catalog,
        &spec.database,
        &spec.table,
        &spec.updated_cols,
        FkTriggerType::OnUpdate,
        plan_ids,
    );
    let updated: Vec<String> = spec
        .updated_cols
        .iter()
        .map(|col| col.to_ascii_lowercase())
        .collect();
    for fk in declared_foreign_keys(catalog, &spec.database, &spec.table) {
        let touches_fk_col = fk
            .cols
            .iter()
            .any(|col| updated.contains(&col.to_ascii_lowercase()));
        if touches_fk_col {
            if let Some(node) = build_fk_check_on_modify_child_table(
                catalog,
                &spec.database,
                &spec.table,
                &fk,
                plan_ids,
            ) {
                nodes.push(node);
            }
        }
    }
    nodes
}

/// Go `BuildOnDeleteFKTriggers`: referred-side triggers only.
fn build_on_delete_fk_triggers(
    catalog: &Catalog,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Vec<FkTriggerNode> {
    build_referred_fk_triggers(
        catalog,
        &spec.database,
        &spec.table,
        &[],
        FkTriggerType::OnDelete,
        plan_ids,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FkTriggerType {
    OnDelete,
    OnUpdate,
}

impl FkTriggerType {
    /// The `FkAction` the refer option is read from, Go `FKCascadeOnDelete`
    /// reading `fk.OnDelete` and `FKCascadeOnUpdate` reading `fk.OnUpdate`.
    fn action<'a>(&self, on_delete: &'a crate::kv_table::table_meta::FkAction, on_update: &'a crate::kv_table::table_meta::FkAction) -> &'a crate::kv_table::table_meta::FkAction {
        match self {
            Self::OnDelete => on_delete,
            Self::OnUpdate => on_update,
        }
    }

    fn info_key(self) -> &'static str {
        match self {
            Self::OnDelete => "on_delete",
            Self::OnUpdate => "on_update",
        }
    }
}

/// Go `buildOnUpdateReferredFKTriggers` / the referred half of
/// `BuildOnDeleteFKTriggers`: for every child constraint citing this table,
/// either a cascade leaf (CASCADE / SET NULL) or a `check_not_exist` leaf.
/// `filter_cols` narrows the UPDATE case to constraints whose REFERENCED
/// columns the SET list touches; an empty slice keeps every referred FK.
fn build_referred_fk_triggers(
    catalog: &Catalog,
    database: &str,
    table: &str,
    filter_cols: &[String],
    trigger: FkTriggerType,
    plan_ids: &PlanIdAllocator,
) -> Vec<FkTriggerNode> {
    let mut nodes = Vec::new();
    for (child_db, child_table, fk) in crate::foreign_key::referring(catalog, database, table) {
        if !filter_cols.is_empty() {
            let touched = fk
                .ref_cols
                .iter()
                .any(|col| filter_cols.iter().any(|f| f.eq_ignore_ascii_case(col)));
            if !touched {
                continue;
            }
        }
        // Go `buildOnDeleteOrUpdateFKTrigger`: resolve the child's own
        // constraint, then split cascade-vs-check on the refer option.
        let child_fks = declared_foreign_keys(catalog, &child_db, &child_table);
        let Some(child_fk) = child_fks
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&fk.name))
        else {
            continue;
        };
        let action = trigger.action(&child_fk.on_delete, &child_fk.on_update);
        if matches!(
            action,
            crate::kv_table::table_meta::FkAction::Cascade
                | crate::kv_table::table_meta::FkAction::SetNull
        ) {
            if let Some(node) = build_fk_cascade(
                catalog,
                &child_db,
                &child_table,
                child_fk,
                trigger,
                plan_ids,
            ) {
                nodes.push(node);
            }
        } else if let Some(node) =
            build_fk_check_for_referred_fk(catalog, &child_db, &child_table, child_fk, plan_ids)
        {
            nodes.push(node);
        }
    }
    nodes
}

/// Go `buildFKCheckOnModifyChildTable`: the child-side `check_exist` leaf
/// whose access object names the REFERRED table and its covering index.
fn build_fk_check_on_modify_child_table(
    catalog: &Catalog,
    _database: &str,
    _table: &str,
    fk: &KvForeignKey,
    plan_ids: &PlanIdAllocator,
) -> Option<FkTriggerNode> {
    // Go: `referTable, err := is.TableByName(fk.RefSchema, fk.RefTable);
    // if err != nil { return nil, nil }` -- a missing parent renders nothing.
    catalog.get_in(&fk.ref_schema, &fk.ref_table)?;
    let (access, _idx) = build_fk_check_access(catalog, &fk.ref_schema, &fk.ref_table, &fk.ref_cols)?;
    Some(FkTriggerNode {
        id: plan_ids.alloc(),
        operator: FK_CHECK,
        access,
        info: format!("foreign_key:{}, check_exist", fk.name),
    })
}

/// Go `buildFKCheckForReferredFK`: the parent-side `check_not_exist` leaf
/// over the CHILD table's index on its own FK columns.
fn build_fk_check_for_referred_fk(
    catalog: &Catalog,
    child_db: &str,
    child_table: &str,
    fk: &KvForeignKey,
    plan_ids: &PlanIdAllocator,
) -> Option<FkTriggerNode> {
    let (access, _idx) = build_fk_check_access(catalog, child_db, child_table, &fk.cols)?;
    Some(FkTriggerNode {
        id: plan_ids.alloc(),
        operator: FK_CHECK,
        access,
        info: format!("foreign_key:{}, check_not_exist", fk.name),
    })
}

/// Go `buildFKCascade`: the cascade leaf over the child table, rendering the
/// refer option verbatim (`on_delete:CASCADE` / `on_update:...`).
fn build_fk_cascade(
    catalog: &Catalog,
    child_db: &str,
    child_table: &str,
    fk: &KvForeignKey,
    trigger: FkTriggerType,
    plan_ids: &PlanIdAllocator,
) -> Option<FkTriggerNode> {
    let entry = catalog.get_in(child_db, child_table)?;
    let TableEntry::Kv(kv) = entry else {
        return None;
    };
    let mut access = format!("table:{child_table}");
    // Go: PKIsHandle + single integer-pk column skips the index lookup;
    // otherwise the FK needs `FindIndexByColumnsForForeignKey` to succeed or
    // the statement errors. Here a missing index simply renders no index
    // suffix, matching `FKCascade.AccessObject` when `FKIdx == nil`.
    if !pk_handle_covers(kv, &fk.cols) {
        let index_name = index_covering(kv, &fk.cols)?;
        access.push_str(&format!(", index:{index_name}"));
    }
    Some(FkTriggerNode {
        id: plan_ids.alloc(),
        operator: FK_CASCADE,
        access,
        info: format!(
            "foreign_key:{}, {}:{}",
            fk.name,
            trigger.info_key(),
            action_string(trigger.action(&fk.on_delete, &fk.on_update)),
        ),
    })
}

/// Go `buildFKCheck`'s access object: `table:<name>` when the single FK
/// column IS the table's integer primary-key handle, else
/// `table:<name>, index:<covering index>`; `None` when no covering index
/// exists, which Go surfaces as `ErrNoReferencedRow2` planning failure.
fn build_fk_check_access(
    catalog: &Catalog,
    database: &str,
    table: &str,
    cols: &[String],
) -> Option<(String, Option<String>)> {
    let entry = catalog.get_in(database, table)?;
    let TableEntry::Kv(kv) = entry else {
        return None;
    };
    if pk_handle_covers(kv, cols) {
        return Some((format!("table:{table}"), None));
    }
    let index_name = index_covering(kv, cols)?;
    Some((format!("table:{table}, index:{index_name}"), Some(index_name)))
}

/// Go `tblInfo.PKIsHandle && len(cols) == 1 &&
/// mysql.HasPriKeyFlag(FindColumnInfo(cols[0]).GetFlag())`.
fn pk_handle_covers(kv: &crate::kv_table::KvTable, cols: &[String]) -> bool {
    if cols.len() != 1 {
        return false;
    }
    kv.columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.name.eq_ignore_ascii_case(&cols[0]))
        .is_some_and(|(offset, _)| kv.is_clustered_handle_column(offset))
}

/// Go `model.FindIndexByColumnsForForeignKey`: the first visible index whose
/// column list equals the foreign-key columns exactly.
fn index_covering(kv: &crate::kv_table::KvTable, cols: &[String]) -> Option<String> {
    let name_at = |offset: usize| kv.columns.get(offset).map(|column| column.name.clone());
    kv.indexes()
        .iter()
        .filter(|index| index.visible)
        .find(|index| {
            index.column_offsets.len() == cols.len()
                && index
                    .column_offsets
                    .iter()
                    .zip(cols)
                    .all(|(offset, col)| {
                        name_at(*offset).is_some_and(|name| name.eq_ignore_ascii_case(col))
                    })
        })
        .map(|index| index.name.clone())
}

/// Go `ast.ReferOptionType.String()` over the stored `FkAction`.
fn action_string(action: &crate::kv_table::table_meta::FkAction) -> &'static str {
    use crate::kv_table::table_meta::FkAction;
    match action {
        FkAction::NoOption => "",
        FkAction::Restrict => "RESTRICT",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::NoAction => "NO ACTION",
        FkAction::SetDefault => "SET DEFAULT",
    }
}
