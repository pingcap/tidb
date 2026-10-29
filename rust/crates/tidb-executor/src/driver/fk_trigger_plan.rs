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

//! The in-process catalog as the planner's [`ForeignKeyCatalog`]: Go
//! `physicalop/foreign_key.go` itself is `tidb_planner::fk_plan`.

use tidb_infoschema::{Catalog, TableEntry};
use crate::driver::DriverError;
use tidb_table::kv_table::KvForeignKey;
use tidb_table::kv_table::FkAction;
use crate::StmtContext;
use tidb_planner::fk_plan::{FkColumn, FkMeta, FkPlanError, FkTable, ForeignKeyCatalog};
use tidb_planner::physical::FkTriggerNode;
use tidb_planner_core_base::PlanIdAllocator;

pub(crate) use tidb_planner::fk_plan::FkPlanSpec;

fn action(action: FkAction) -> tidb_planner::fk_plan::FkAction {
    use tidb_planner::fk_plan::FkAction as Planned;
    match action {
        FkAction::NoOption => Planned::NoOption,
        FkAction::Restrict => Planned::Restrict,
        FkAction::Cascade => Planned::Cascade,
        FkAction::SetNull => Planned::SetNull,
        FkAction::NoAction => Planned::NoAction,
        FkAction::SetDefault => Planned::SetDefault,
    }
}

pub(crate) fn meta(fk: &KvForeignKey) -> FkMeta {
    FkMeta {
        version: fk.version,
        name: fk.name.clone(),
        cols: fk.cols.clone(),
        ref_schema: fk.ref_schema.clone(),
        ref_table: fk.ref_table.clone(),
        ref_cols: fk.ref_cols.clone(),
        on_delete: action(fk.on_delete),
        on_update: action(fk.on_update),
    }
}

struct CatalogForeignKeys<'a>(&'a Catalog);

impl ForeignKeyCatalog for CatalogForeignKeys<'_> {
    fn table(&self, database: &str, table: &str) -> Option<FkTable> {
        let Some(TableEntry::Kv(table)) = self.0.get_in(database, table) else {
            return None;
        };
        Some(FkTable {
            columns: table
                .columns()
                .iter()
                .map(|column| FkColumn {
                    name: column.name.clone(),
                    on_update_now: column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW),
                    generated_dependencies: column
                        .generated
                        .as_ref()
                        .map(|generated| generated.dependencies.clone()),
                })
                .collect(),
            foreign_keys: table.foreign_keys().iter().map(meta).collect(),
        })
    }

    fn referring(&self, database: &str, table: &str) -> Vec<(String, String, FkMeta)> {
        crate::foreign_key::referring(self.0, database, table)
            .into_iter()
            .map(|(child_db, child_table, fk)| (child_db, child_table, meta(&fk)))
            .collect()
    }

    fn lookup_index(
        &self,
        database: &str,
        table: &str,
        columns: &[usize],
    ) -> Option<Option<(i64, String)>> {
        let Some(TableEntry::Kv(table)) = self.0.get_in(database, table) else {
            return None;
        };
        table.foreign_key_lookup_index(columns).map(|index| {
            index.map(|id| {
                let found = table.indexes().iter().find(|index| index.id == id).unwrap();
                (id, found.name.clone())
            })
        })
    }
}

/// Select constraints once, following Go's BuildOn{Insert,Update,Delete}FKTriggers.
pub(crate) fn build_fk_triggers(
    catalog: &Catalog,
    ctx: &StmtContext,
    operator: &str,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Result<Vec<FkTriggerNode>, DriverError> {
    tidb_planner::fk_plan::build_fk_triggers(
        &CatalogForeignKeys(catalog),
        ctx.foreign_key_checks(),
        operator,
        spec,
        plan_ids,
    )
    .map_err(|error| match error {
        FkPlanError::NoReferencedRow(fk) => DriverError::from(
            tidb_plannererrors::ERR_NO_REFERENCED_ROW2.fast_generate_by_args(&[fk.into()]),
        ),
        FkPlanError::RowIsReferenced(fk) => DriverError::from(
            tidb_plannererrors::ERR_ROW_IS_REFERENCED2
                .generate_with_stack_by_args(&[fk.into()]),
        ),
        FkPlanError::Unsupported(message) => DriverError::unsupported(message),
    })
}
