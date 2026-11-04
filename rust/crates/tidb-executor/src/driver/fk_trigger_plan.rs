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
//! `physicalop/foreign_key.go` itself is `tidb_planner_core_operator_physicalop::fk_plan`.

use tidb_infoschema::{SessionExtendedInfoSchema, TableEntry};
use crate::driver::DriverError;
use crate::StmtContext;
use tidb_planner_core_operator_physicalop::fk_plan::{FkPlanError, ForeignKeyCatalog};
use tidb_planner_core_operator_physicalop::FkTriggerNode;
use tidb_planner_core_base::PlanIdAllocator;

pub(crate) use tidb_planner_core_operator_physicalop::fk_plan::FkPlanSpec;

struct CatalogForeignKeys<'a>(&'a SessionExtendedInfoSchema);

impl ForeignKeyCatalog for CatalogForeignKeys<'_> {
    fn table(
        &self,
        database: &str,
        table: &str,
    ) -> Option<tidb_model::GoShared<tidb_model::table_info::TableInfo>> {
        let Some(TableEntry::Kv(table)) = self.0.get_in(database, table) else {
            return None;
        };
        Some(table.meta())
    }

    fn referring(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<(String, String, tidb_model::GoShared<tidb_model::table::FKInfo>)> {
        crate::foreign_key::referring(self.0, database, table)
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
                let found = table.indexes().iter().find(|index| index.read().id == id).unwrap();
                (id, found.read().name.original().to_owned())
            })
        })
    }
}

/// Select constraints once, following Go's BuildOn{Insert,Update,Delete}FKTriggers.
pub(crate) fn build_fk_triggers(
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
    operator: &str,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Result<Vec<FkTriggerNode>, DriverError> {
    // The updated columns are those of Go's write lists: the SET (or ON
    // DUPLICATE) assignments plus the generated assignments the planner
    // appends to them (`Update.OrderedList`, `Insert.OnDuplicate`).
    let mut spec = spec.clone();
    if let Some(TableEntry::Kv(table)) = catalog.get_in(&spec.database, &spec.table) {
        let columns: Vec<_> = table.meta().read().cols().iter_handles().flatten().collect();
        use tidb_planner::plan_builder::generated_assignments::{
            on_duplicate_generated_columns, update_list_generated_columns,
        };
        if !spec.updated_cols.is_empty() {
            let generated = update_list_generated_columns(&columns, &spec.updated_cols);
            spec.updated_cols.extend(generated);
        }
        if !spec.on_duplicate_cols.is_empty() {
            let generated = on_duplicate_generated_columns(&columns, &spec.on_duplicate_cols);
            spec.on_duplicate_cols.extend(generated);
        }
    }
    tidb_planner_core_operator_physicalop::fk_plan::build_fk_triggers(
        &CatalogForeignKeys(catalog),
        ctx.foreign_key_checks(),
        operator,
        &spec,
        plan_ids,
    )
    .map_err(|error| match error {
        FkPlanError::NoReferencedRow(fk) => DriverError::from(
            tidb_plannererrors::ERR_NO_REFERENCED_ROW2.fast_gen_by_args(&[fk.into()]),
        ),
        FkPlanError::RowIsReferenced(fk) => DriverError::from(
            tidb_plannererrors::ERR_ROW_IS_REFERENCED2
                .gen_with_stack_by_args(&[fk.into()]),
        ),
        FkPlanError::Unsupported(message) => DriverError::unsupported(message),
    })
}
