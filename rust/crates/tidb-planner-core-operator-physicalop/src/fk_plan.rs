// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `physicalop/foreign_key.go`: `BuildOnInsertFKTriggers`,
//! `BuildOnUpdateFKTriggers` and `BuildOnDeleteFKTriggers`, which every
//! INSERT, UPDATE and DELETE builder runs last. The checks and cascades they
//! select are retained once on the DML root and consumed by both execution and
//! EXPLAIN.
//!
//! The foreign key metadata is reached through [`ForeignKeyCatalog`], the part
//! of Go's `table.Table` and `infoschema.InfoSchema` these functions read.

use tidb_ast::{CiString, ReferOptionType};
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_datatype::FieldTypeFlags;
use tidb_model::column::ColumnInfo;
use tidb_model::table::FKInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::GoShared;

use crate::{FkTriggerKind, FkTriggerNode};
use tidb_planner_core_base::PlanIdAllocator;

/// Go `tbl.Cols()`: the table's public columns in offset order, the row
/// layout the triggers' offsets index.
fn public_columns(table: &GoShared<TableInfo>) -> Vec<GoShared<ColumnInfo>> {
    table.read().cols().iter_handles().flatten().collect()
}

/// The metadata `BuildOn*FKTriggers` reads.
pub trait ForeignKeyCatalog {
    /// Go `is.TableByName(...).Meta()` of a base table, or `None` for any
    /// other kind or a missing one.
    fn table(&self, database: &str, table: &str) -> Option<GoShared<TableInfo>>;

    /// Go `is.GetTableReferredForeignKeys`: the constraints of other tables
    /// that reference `table`, as `(child schema, child table, constraint)`,
    /// in schema/table then declaration order.
    fn referring(&self, database: &str, table: &str) -> Vec<(String, String, GoShared<FKInfo>)>;

    /// Go `FindIndexByColumnsForForeignKey` over the table's columns at
    /// `columns` (offsets). The outer `None` is a missing access path, the
    /// inner `None` an integer clustered key that needs no secondary index;
    /// otherwise the index id and name.
    fn lookup_index(
        &self,
        database: &str,
        table: &str,
        columns: &[usize],
    ) -> Option<Option<(i64, String)>>;
}

/// What a DML statement asks of the trigger builder for one affected table.
#[derive(Clone, Debug, Default)]
pub struct FkPlanSpec {
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

/// Why a trigger could not be planned.
#[derive(Clone, Debug)]
pub enum FkPlanError {
    /// Go `ErrNoReferencedRow2`: a child-side check has no access path;
    /// the payload is the foreign key's `FKInfo.String`.
    NoReferencedRow(String),
    /// Go `ErrRowIsReferenced2`: a parent-side check has no access path;
    /// the payload is the foreign key's `FKInfo.String`.
    RowIsReferenced(String),
    /// A message-only refusal.
    Unsupported(String),
}

/// Select constraints once, following Go's BuildOn{Insert,Update,Delete}FKTriggers.
pub fn build_fk_triggers(
    catalog: &dyn ForeignKeyCatalog,
    foreign_key_checks: bool,
    operator: &str,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Result<Vec<FkTriggerNode>, FkPlanError> {
    if !foreign_key_checks {
        return Ok(Vec::new());
    }
    // Go buildUpdateLists appends generated assignments when their
    // dependencies may change, including transitive generated dependencies
    // and ON UPDATE timestamps. ODKU uses the same completed update columns.
    let mut spec = spec.clone();
    let own_table = catalog.table(&spec.database, &spec.table);
    if let Some(table) = &own_table {
        let columns = public_columns(table);
        for updated in [&mut spec.updated_cols, &mut spec.on_duplicate_cols] {
            if updated.is_empty() {
                continue;
            }
            let mut modified = updated.clone();
            modified.extend(
                columns
                    .iter()
                    .map(|column| column.read())
                    .filter(|column| column.field_type.has_flag(FieldTypeFlags::ON_UPDATE_NOW))
                    .map(|column| column.name.original().to_owned()),
            );
            for column in &columns {
                let column = column.read();
                if column.is_generated()
                    && column.dependences.snapshot().iter().any(|dependency| {
                        let dependency = dependency.to_utf8_lossy_go();
                        modified.iter().any(|name| name.go_lower_eq(&dependency))
                    })
                {
                    let name = column.name.original();
                    if !modified.iter().any(|modified| modified.go_lower_eq(name)) {
                        modified.push(name.to_owned());
                    }
                    if !updated.iter().any(|updated| updated.go_lower_eq(name)) {
                        updated.push(name.to_owned());
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
        for (child_db, child_table, fk) in catalog.referring(&spec.database, &spec.table) {
            let fk = fk.read();
            if !on_delete && !touches(updated, &fk.ref_cols.snapshot()) {
                continue;
            }
            let action = ReferOptionType::from(if on_delete {
                fk.on_delete
            } else {
                fk.on_update
            });
            let kind = match action {
                ReferOptionType::Cascade | ReferOptionType::SetNull => FkTriggerKind::Cascade {
                    on_delete,
                    set_null: action == ReferOptionType::SetNull,
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
    if let Some(table) = &own_table {
        let foreign_keys = table
            .read()
            .foreign_keys
            .iter_deref()
            .filter(|fk| fk.read().state == tidb_model::SchemaState::PUBLIC)
            .collect::<Vec<_>>();
        for fk in &foreign_keys {
            let fk = fk.read();
            if fk.version < tidb_model::table::FK_VERSION1 {
                continue;
            }
            if operator != "Insert"
                && (operator != "Update" || !touches(&spec.updated_cols, &fk.cols.snapshot()))
            {
                continue;
            }
            if let Some(node) = compile_trigger(
                catalog,
                &spec.database,
                &spec.table,
                &fk,
                FkTriggerKind::ChildCheck,
                ReferOptionType::NoOption,
                plan_ids,
            )? {
                nodes.push(node);
            }
        }
    }
    Ok(nodes)
}

/// Go `isMapContainAnyCols`.
fn touches(updated: &[String], columns: &[CiString]) -> bool {
    columns
        .iter()
        .any(|column| updated.iter().any(|name| name.go_lower_eq(column.lowercase())))
}

fn compile_trigger(
    catalog: &dyn ForeignKeyCatalog,
    child_db: &str,
    child_table: &str,
    fk: &FKInfo,
    kind: FkTriggerKind,
    action: ReferOptionType,
    plan_ids: &PlanIdAllocator,
) -> Result<Option<FkTriggerNode>, FkPlanError> {
    let Some(parent) = catalog.table(fk.ref_schema.original(), fk.ref_table.original()) else {
        // Go buildFKCheckOnModifyChildTable omits a missing parent.
        return Ok(None);
    };
    let Some(child) = catalog.table(child_db, child_table) else {
        return Ok(None);
    };
    let offsets = |table: &GoShared<TableInfo>, names: &[CiString]| -> Result<Vec<usize>, FkPlanError> {
        let columns = public_columns(table);
        names
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .position(|column| column.read().name.lowercase() == name.lowercase())
                    .ok_or_else(|| {
                        FkPlanError::Unsupported(format!("foreign key column {name} is not found"))
                    })
            })
            .collect()
    };
    let child_offsets = offsets(&child, &fk.cols.snapshot())?;
    let parent_offsets = offsets(&parent, &fk.ref_cols.snapshot())?;
    // Go `fk.String(dbName, tblInfo.Name.L)` / `(ChildSchema.L, ChildTable.L)`.
    let fk_string = fk.string(&child_db.go_to_lower(), &child_table.go_to_lower());
    let failure = || {
        if kind == FkTriggerKind::ChildCheck {
            FkPlanError::NoReferencedRow(fk_string.clone())
        } else {
            FkPlanError::RowIsReferenced(fk_string.clone())
        }
    };
    let (table_schema, table_name, columns) = if kind == FkTriggerKind::ChildCheck {
        (
            fk.ref_schema.original(),
            fk.ref_table.original(),
            &parent_offsets,
        )
    } else {
        (child_db, child_table, &child_offsets)
    };
    let mut access = format!("table:{table_name}");
    let Some(lookup_index) = catalog.lookup_index(table_schema, table_name, columns) else {
        return Err(if matches!(kind, FkTriggerKind::Cascade { .. }) {
            FkPlanError::Unsupported(format!(
                "Missing index for '{}' foreign key columns in the table '{}'",
                fk.name.original(),
                child_table
            ))
        } else {
            failure()
        });
    };
    let lookup_id = lookup_index.as_ref().map(|(id, _)| *id);
    if let Some((_, index_name)) = &lookup_index {
        access.push_str(&format!(", index:{index_name}"));
    }
    let (operator, info) = match kind {
        FkTriggerKind::ChildCheck => (
            "Foreign_Key_Check",
            format!("foreign_key:{}, check_exist", fk.name.original()),
        ),
        FkTriggerKind::ParentCheck => (
            "Foreign_Key_Check",
            format!("foreign_key:{}, check_not_exist", fk.name.original()),
        ),
        FkTriggerKind::Cascade { on_delete, .. } => (
            "Foreign_Key_Cascade",
            format!(
                "foreign_key:{}, {}:{}",
                fk.name.original(),
                if on_delete { "on_delete" } else { "on_update" },
                if action == ReferOptionType::SetNull {
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
        parent_database: fk.ref_schema.original().to_owned(),
        parent_table: fk.ref_table.original().to_owned(),
        child_offsets,
        parent_offsets,
        lookup_index: lookup_id,
        fk_string,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::FkTriggerKind;

    /// `parent(id)` referenced by `child(pid)` with `fk_child (pid) REFERENCES
    /// parent (id)`, the child indexed on `pid`.
    struct Fake {
        tables: BTreeMap<(String, String), GoShared<TableInfo>>,
        indexed: Vec<(String, String)>,
        action: ReferOptionType,
    }

    /// Go `model.TableInfo` with public `columns` and `foreign_keys`.
    fn table(columns: &[&str], foreign_keys: Vec<GoShared<FKInfo>>) -> GoShared<TableInfo> {
        GoShared::new(TableInfo {
            columns: columns
                .iter()
                .enumerate()
                .map(|(offset, name)| ColumnInfo {
                    id: offset as i64 + 1,
                    name: CiString::new(*name),
                    offset: offset as i64,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..ColumnInfo::default()
                })
                .collect::<Vec<_>>()
                .into(),
            foreign_keys: tidb_model::GoSharedPointerSlice::from_handles(
                foreign_keys.into_iter().map(Some).collect(),
            ),
            ..TableInfo::default()
        })
    }

    fn fk(action: ReferOptionType) -> GoShared<FKInfo> {
        GoShared::new(FKInfo {
            version: tidb_model::table::FK_VERSION1,
            name: CiString::new("fk_child"),
            cols: vec![CiString::new("pid")].into(),
            ref_schema: CiString::new("test"),
            ref_table: CiString::new("parent"),
            ref_cols: vec![CiString::new("id")].into(),
            on_delete: action.into(),
            on_update: action.into(),
            state: tidb_model::SchemaState::PUBLIC,
            ..FKInfo::default()
        })
    }

    fn fake(action: ReferOptionType, indexed: &[&str]) -> Fake {
        let mut tables = BTreeMap::new();
        tables.insert(
            ("test".to_owned(), "parent".to_owned()),
            table(&["id"], Vec::new()),
        );
        tables.insert(
            ("test".to_owned(), "child".to_owned()),
            table(&["id", "pid"], vec![fk(action)]),
        );
        Fake {
            tables,
            indexed: indexed
                .iter()
                .map(|name| ("test".to_owned(), (*name).to_owned()))
                .collect(),
            action,
        }
    }

    impl ForeignKeyCatalog for Fake {
        fn table(&self, database: &str, table: &str) -> Option<GoShared<TableInfo>> {
            self.tables
                .get(&(database.to_owned(), table.to_owned()))
                .cloned()
        }

        fn referring(&self, database: &str, table: &str) -> Vec<(String, String, GoShared<FKInfo>)> {
            if (database, table) == ("test", "parent") {
                vec![("test".to_owned(), "child".to_owned(), fk(self.action))]
            } else {
                Vec::new()
            }
        }

        fn lookup_index(
            &self,
            database: &str,
            table: &str,
            _columns: &[usize],
        ) -> Option<Option<(i64, String)>> {
            self.indexed
                .contains(&(database.to_owned(), table.to_owned()))
                .then(|| Some((7, format!("idx_{table}"))))
        }
    }

    fn spec(table: &str) -> FkPlanSpec {
        FkPlanSpec {
            database: "test".to_owned(),
            table: table.to_owned(),
            ..FkPlanSpec::default()
        }
    }

    #[test]
    fn deleting_a_parent_row_selects_the_cascade_or_the_not_exist_check() {
        let ids = PlanIdAllocator::new();
        let cascade = build_fk_triggers(
            &fake(ReferOptionType::Cascade, &["child", "parent"]),
            true,
            "Delete",
            &spec("parent"),
            &ids,
        )
        .unwrap();
        assert_eq!(cascade.len(), 1);
        assert_eq!(cascade[0].operator, "Foreign_Key_Cascade");
        assert_eq!(cascade[0].info, "foreign_key:fk_child, on_delete:CASCADE");
        assert_eq!(cascade[0].access, "table:child, index:idx_child");
        assert!(matches!(
            cascade[0].kind,
            FkTriggerKind::Cascade {
                on_delete: true,
                set_null: false
            }
        ));
        let restrict = build_fk_triggers(
            &fake(ReferOptionType::Restrict, &["child"]),
            true,
            "Delete",
            &spec("parent"),
            &ids,
        )
        .unwrap();
        assert_eq!(restrict[0].operator, "Foreign_Key_Check");
        assert_eq!(restrict[0].info, "foreign_key:fk_child, check_not_exist");
    }

    #[test]
    fn writing_a_child_checks_the_parent_only_for_touched_columns() {
        let ids = PlanIdAllocator::new();
        let catalog = fake(ReferOptionType::Restrict, &["parent"]);
        let insert = build_fk_triggers(&catalog, true, "Insert", &spec("child"), &ids).unwrap();
        assert_eq!(insert[0].info, "foreign_key:fk_child, check_exist");
        assert_eq!(insert[0].access, "table:parent, index:idx_parent");
        let mut update = spec("child");
        update.updated_cols = vec!["ID".to_owned()];
        assert!(build_fk_triggers(&catalog, true, "Update", &update, &ids)
            .unwrap()
            .is_empty());
        update.updated_cols = vec!["PID".to_owned()];
        assert_eq!(
            build_fk_triggers(&catalog, true, "Update", &update, &ids)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_disabled_check_or_a_missing_index_changes_the_outcome() {
        let ids = PlanIdAllocator::new();
        assert!(build_fk_triggers(
            &fake(ReferOptionType::Cascade, &["child"]),
            false,
            "Delete",
            &spec("parent"),
            &ids
        )
        .unwrap()
        .is_empty());
        // No index on the child: a cascade is refused by message, a check by
        // the constraint violation Go reports.
        assert!(matches!(
            build_fk_triggers(&fake(ReferOptionType::Cascade, &[]), true, "Delete", &spec("parent"), &ids),
            Err(FkPlanError::Unsupported(message)) if message.contains("Missing index")
        ));
        assert!(matches!(
            build_fk_triggers(
                &fake(ReferOptionType::Restrict, &[]),
                true,
                "Delete",
                &spec("parent"),
                &ids
            ),
            Err(FkPlanError::RowIsReferenced(_))
        ));
        assert!(matches!(
            build_fk_triggers(
                &fake(ReferOptionType::Restrict, &[]),
                true,
                "Insert",
                &spec("child"),
                &ids
            ),
            Err(FkPlanError::NoReferencedRow(_))
        ));
    }

    #[test]
    fn a_generated_column_joins_the_update_columns_it_depends_on() {
        let ids = PlanIdAllocator::new();
        let catalog = fake(ReferOptionType::Restrict, &["parent", "child"]);
        let child = &catalog.tables[&("test".to_owned(), "child".to_owned())];
        // `pid` becomes generated from `id`: updating `id` moves `pid`, which
        // the foreign key reads.
        let pid = child.read().columns.get(1).unwrap();
        let mut pid = pid.write();
        pid.generated_expr_string = "`id`".to_owned();
        pid.dependences.insert("id");
        drop(pid);
        let mut update = spec("child");
        update.updated_cols = vec!["id".to_owned()];
        assert_eq!(
            build_fk_triggers(&catalog, true, "Update", &update, &ids)
                .unwrap()
                .len(),
            1
        );
    }
}
