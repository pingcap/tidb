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

use tidb_hack::{GoCaseCompare, GoToLower};

use crate::physical::{FkTriggerKind, FkTriggerNode};
use crate::plan_base::PlanIdAllocator;

/// Go `model.ReferOptionType`, as a foreign key's `ON DELETE` / `ON UPDATE`
/// action.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FkAction {
    /// No clause was supplied; behaves as `RESTRICT`.
    #[default]
    NoOption,
    /// Reject the parent mutation while a referencing row exists.
    Restrict,
    /// Delete the referencing rows, or repoint them at the new value.
    Cascade,
    /// Null out the referencing columns.
    SetNull,
    /// Stored like MySQL's spelling but not deferred.
    NoAction,
    /// Parsed and stored; never enforced.
    SetDefault,
}

/// Go `model.FKInfo`.
#[derive(Clone, Debug)]
pub struct FkMeta {
    /// Go `Version`: zero retains metadata without enforcement.
    pub version: i64,
    /// The constraint name.
    pub name: String,
    /// The referencing columns' names.
    pub cols: Vec<String>,
    /// The referenced schema.
    pub ref_schema: String,
    /// The referenced table.
    pub ref_table: String,
    /// The referenced columns' names, parallel to `cols`.
    pub ref_cols: Vec<String>,
    /// Go `OnDelete`.
    pub on_delete: FkAction,
    /// Go `OnUpdate`.
    pub on_update: FkAction,
}

impl FkMeta {
    /// Go `FKInfo.String(db, tb)`: the child table and the constraint as a
    /// violation reports them. The referenced schema and table are
    /// lower-cased, and the schema is omitted when it equals `db`.
    #[must_use]
    pub fn string(&self, db: &str, tb: &str) -> String {
        let quoted = |cols: &[String]| {
            cols.iter()
                .map(|col| format!("`{col}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ref_schema = self.ref_schema.to_lowercase();
        let mut buf = format!(
            "`{db}`.`{tb}`, CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES `",
            self.name,
            quoted(&self.cols)
        );
        if ref_schema != db {
            buf.push_str(&ref_schema);
            buf.push_str("`.`");
        }
        buf.push_str(&self.ref_table.to_lowercase());
        buf.push_str("` (");
        buf.push_str(&quoted(&self.ref_cols));
        buf.push(')');
        if let Some(action) = self.on_delete.refer_option_string() {
            buf.push_str(" ON DELETE ");
            buf.push_str(action);
        }
        if let Some(action) = self.on_update.refer_option_string() {
            buf.push_str(" ON UPDATE ");
            buf.push_str(action);
        }
        buf
    }
}

impl FkAction {
    /// Go `ReferOptionType.String`, `None` for `ReferOptionNoOption`.
    #[must_use]
    pub const fn refer_option_string(self) -> Option<&'static str> {
        match self {
            Self::NoOption => None,
            Self::Restrict => Some("RESTRICT"),
            Self::Cascade => Some("CASCADE"),
            Self::SetNull => Some("SET NULL"),
            Self::NoAction => Some("NO ACTION"),
            Self::SetDefault => Some("SET DEFAULT"),
        }
    }
}

/// A column of a table, as the trigger builders read it.
#[derive(Clone, Debug)]
pub struct FkColumn {
    /// The column name.
    pub name: String,
    /// Whether the column carries `ON UPDATE CURRENT_TIMESTAMP`.
    pub on_update_now: bool,
    /// The columns a generated column reads, `None` for an ordinary column.
    pub generated_dependencies: Option<Vec<String>>,
}

/// A table with its columns and its own foreign keys.
#[derive(Clone, Debug)]
pub struct FkTable {
    /// Go `Meta().Columns`.
    pub columns: Vec<FkColumn>,
    /// Go `Meta().ForeignKeys`.
    pub foreign_keys: Vec<FkMeta>,
}

/// The metadata `BuildOn*FKTriggers` reads.
pub trait ForeignKeyCatalog {
    /// A base table, or `None` for any other kind or a missing one.
    fn table(&self, database: &str, table: &str) -> Option<FkTable>;

    /// Go `is.GetTableReferredForeignKeys`: the constraints of other tables
    /// that reference `table`, as `(child schema, child table, constraint)`,
    /// in schema/table then declaration order.
    fn referring(&self, database: &str, table: &str) -> Vec<(String, String, FkMeta)>;

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
        for updated in [&mut spec.updated_cols, &mut spec.on_duplicate_cols] {
            if updated.is_empty() {
                continue;
            }
            let mut modified = updated.clone();
            modified.extend(
                table
                    .columns
                    .iter()
                    .filter(|column| column.on_update_now)
                    .map(|column| column.name.clone()),
            );
            for column in &table.columns {
                if column
                    .generated_dependencies
                    .as_ref()
                    .is_some_and(|dependencies| touches(&modified, dependencies))
                {
                    if !modified.iter().any(|name| name.go_lower_eq(&column.name)) {
                        modified.push(column.name.clone());
                    }
                    if !updated.iter().any(|name| name.go_lower_eq(&column.name)) {
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
        for (child_db, child_table, fk) in catalog.referring(&spec.database, &spec.table) {
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
    if let Some(table) = &own_table {
        for fk in &table.foreign_keys {
            if fk.version < tidb_model::table::FK_VERSION1 {
                continue;
            }
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
        .any(|column| updated.iter().any(|name| name.go_lower_eq(column)))
}

fn compile_trigger(
    catalog: &dyn ForeignKeyCatalog,
    child_db: &str,
    child_table: &str,
    fk: &FkMeta,
    kind: FkTriggerKind,
    action: FkAction,
    plan_ids: &PlanIdAllocator,
) -> Result<Option<FkTriggerNode>, FkPlanError> {
    let Some(parent) = catalog.table(&fk.ref_schema, &fk.ref_table) else {
        // Go buildFKCheckOnModifyChildTable omits a missing parent.
        return Ok(None);
    };
    let Some(child) = catalog.table(child_db, child_table) else {
        return Ok(None);
    };
    let offsets = |table: &FkTable, names: &[String]| -> Result<Vec<usize>, FkPlanError> {
        names
            .iter()
            .map(|name| {
                table
                    .columns
                    .iter()
                    .position(|column| column.name.go_lower_eq(name))
                    .ok_or_else(|| {
                        FkPlanError::Unsupported(format!("foreign key column {name} is not found"))
                    })
            })
            .collect()
    };
    let child_offsets = offsets(&child, &fk.cols)?;
    let parent_offsets = offsets(&parent, &fk.ref_cols)?;
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
            fk.ref_schema.as_str(),
            fk.ref_table.as_str(),
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
                fk.name, child_table
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
        lookup_index: lookup_id,
        fk_string,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::physical::FkTriggerKind;

    /// `parent(id)` referenced by `child(pid)` with `fk_child (pid) REFERENCES
    /// parent (id)`, the child indexed on `pid`.
    struct Fake {
        tables: BTreeMap<(String, String), FkTable>,
        indexed: Vec<(String, String)>,
        action: FkAction,
    }

    fn column(name: &str) -> FkColumn {
        FkColumn {
            name: name.to_owned(),
            on_update_now: false,
            generated_dependencies: None,
        }
    }

    fn fk(action: FkAction) -> FkMeta {
        FkMeta {
            version: tidb_model::table::FK_VERSION1,
            name: "fk_child".to_owned(),
            cols: vec!["pid".to_owned()],
            ref_schema: "test".to_owned(),
            ref_table: "parent".to_owned(),
            ref_cols: vec!["id".to_owned()],
            on_delete: action,
            on_update: action,
        }
    }

    fn fake(action: FkAction, indexed: &[&str]) -> Fake {
        let mut tables = BTreeMap::new();
        tables.insert(
            ("test".to_owned(), "parent".to_owned()),
            FkTable {
                columns: vec![column("id")],
                foreign_keys: Vec::new(),
            },
        );
        tables.insert(
            ("test".to_owned(), "child".to_owned()),
            FkTable {
                columns: vec![column("id"), column("pid")],
                foreign_keys: vec![fk(action)],
            },
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
        fn table(&self, database: &str, table: &str) -> Option<FkTable> {
            self.tables
                .get(&(database.to_owned(), table.to_owned()))
                .cloned()
        }

        fn referring(&self, database: &str, table: &str) -> Vec<(String, String, FkMeta)> {
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
            &fake(FkAction::Cascade, &["child", "parent"]),
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
            &fake(FkAction::Restrict, &["child"]),
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
        let catalog = fake(FkAction::Restrict, &["parent"]);
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
            &fake(FkAction::Cascade, &["child"]),
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
            build_fk_triggers(&fake(FkAction::Cascade, &[]), true, "Delete", &spec("parent"), &ids),
            Err(FkPlanError::Unsupported(message)) if message.contains("Missing index")
        ));
        assert!(matches!(
            build_fk_triggers(
                &fake(FkAction::Restrict, &[]),
                true,
                "Delete",
                &spec("parent"),
                &ids
            ),
            Err(FkPlanError::RowIsReferenced(_))
        ));
        assert!(matches!(
            build_fk_triggers(
                &fake(FkAction::Restrict, &[]),
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
        let mut catalog = fake(FkAction::Restrict, &["parent", "child"]);
        let child = catalog
            .tables
            .get_mut(&("test".to_owned(), "child".to_owned()))
            .unwrap();
        // `pid` becomes generated from `id`: updating `id` moves `pid`, which
        // the foreign key reads.
        child.columns[1].generated_dependencies = Some(vec!["id".to_owned()]);
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
