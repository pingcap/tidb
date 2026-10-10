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

//! Go's BDR admission for the DDL this node plans and publishes directly.
//!
//! A persisted job meets Go's `SubmitBatch` admission in
//! [`crate::ddl_job_submit::prepare_spec_for_submit`]. The changes
//! [`crate::cluster_ddl::plan_ddl`] publishes in one transaction have no
//! submitted job, so the planner asks the same questions: the ADD COLUMN /
//! MODIFY COLUMN builders' `IsAddColumnDenied` / `IsModifyColumnDenied`
//! while it builds the column, and the submitter's `IsDenied` over the
//! planned change before it is published. The role is read from the same
//! snapshot the plan reads (Go's `meta.Mutator.GetBDRRole`).

use tidb_ast::BdrRole;
use tidb_model::{ActionType, SchemaDiff};

use crate::cluster_ddl::{AlterColumnAction, DdlPlanError, DdlStatement};
use crate::ddl_job_submit::{submit_error, JobSubmitError};

fn role_of(stored: &[u8]) -> Option<BdrRole> {
    match stored {
        b"primary" => Some(BdrRole::Primary),
        b"secondary" => Some(BdrRole::Secondary),
        _ => None,
    }
}

fn restricted(stored: &[u8]) -> DdlPlanError {
    submit_error(JobSubmitError::BdrRestricted(
        String::from_utf8_lossy(stored).into_owned(),
    ))
}

fn is_system_schema(schema: &str) -> bool {
    tidb_util::filter::is_system_schema(&schema.to_lowercase())
}

/// Go `AddColumn`'s `IsAddColumnDenied` check over the built column.
pub(crate) fn admit_add_column(
    stored: &[u8],
    schema: &str,
    options: &[tidb_ast::ColumnOption],
) -> Result<(), DdlPlanError> {
    if tidb_model::ddl_bdr::is_add_column_denied(role_of(stored), options)
        && !is_system_schema(schema)
    {
        return Err(restricted(stored));
    }
    Ok(())
}

/// Go `GetModifiableColumnJob`'s `IsModifyColumnDenied` check over the
/// built column.
pub(crate) fn admit_modify_column(
    stored: &[u8],
    schema: &str,
    new_field_type: &tidb_datatype::FieldType,
    old_field_type: &tidb_datatype::FieldType,
    options: &[tidb_ast::ColumnOption],
) -> Result<(), DdlPlanError> {
    if tidb_model::ddl_bdr::is_modify_column_denied(
        role_of(stored),
        new_field_type,
        old_field_type,
        options,
    ) && !is_system_schema(schema)
    {
        return Err(restricted(stored));
    }
    Ok(())
}

/// Go `SubmitBatch`'s admission of the planned change: a change not from
/// TiCDC, under a set role, on a schema that is not a system schema, is
/// refused when `bdr.IsDenied` refuses its action -- each sub-action of a
/// multi-schema change.
pub(crate) fn admit_submit(
    stored: &[u8],
    cdc_write_source: u64,
    schema: &str,
    statement: &DdlStatement,
    diff: &SchemaDiff,
) -> Result<(), DdlPlanError> {
    let role = role_of(stored);
    if cdc_write_source != 0 || role.is_none() || is_system_schema(schema) {
        return Ok(());
    }
    let first_index_unique = first_index_unique(statement);
    let denied = |action: ActionType| {
        tidb_model::ddl_bdr::is_action_denied(role, action, first_index_unique)
    };
    let refused = if diff.action_type == ActionType::ACTION_MULTI_SCHEMA_CHANGE {
        diff.sub_action_types.iter().copied().any(denied)
    } else {
        denied(diff.action_type)
    };
    if refused {
        return Err(restricted(stored));
    }
    Ok(())
}

/// Whether the index an ADD INDEX change adds is unique (a primary key is).
/// Go's merged ADD INDEX sub-job carries every added index in order, and the
/// submitter reads the first one's.
fn first_index_unique(statement: &DdlStatement) -> Option<bool> {
    match statement {
        DdlStatement::CreateIndex { index, .. } => Some(index.unique || index.primary),
        DdlStatement::MultiSchemaChange { actions, .. } => actions
            .iter()
            .filter_map(|action| match action {
                AlterColumnAction::AddIndex { index, .. } => Some(index.unique || index.primary),
                _ => None,
            })
            .reduce(|first, _| first),
        _ => None,
    }
}
