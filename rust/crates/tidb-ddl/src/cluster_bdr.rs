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
//! [`tidb_ddl_jobsubmit::prepare_spec_for_submit`]. The changes
//! [`crate::cluster_ddl::plan_ddl`] publishes in one transaction have no
//! submitted job, so the planner asks the same questions: the ADD COLUMN /
//! MODIFY COLUMN builders' `IsAddColumnDenied` / `IsModifyColumnDenied`
//! while it builds the column, and the submitter's `IsDenied` over the
//! planned change before it is published. The role is read from the same
//! snapshot the plan reads (Go's `meta.Mutator.GetBDRRole`).

use tidb_ast::BdrRole;

use tidb_ddl_build::plan_error::DdlPlanError;
use tidb_ddl_jobsubmit::JobSubmitError;
use tidb_hack::GoToLower;

fn role_of(stored: &[u8]) -> Option<BdrRole> {
    match stored {
        b"primary" => Some(BdrRole::Primary),
        b"secondary" => Some(BdrRole::Secondary),
        _ => None,
    }
}

fn restricted(stored: &[u8]) -> DdlPlanError {
    DdlPlanError::from(JobSubmitError::BdrRestricted(
        String::from_utf8_lossy(stored).into_owned(),
    ))
}

fn is_system_schema(schema: &str) -> bool {
    tidb_filter::is_system_schema(&schema.go_to_lower())
}

/// Go `AddColumn`'s `IsAddColumnDenied` check over the built column.
pub(crate) fn admit_add_column(
    stored: &[u8],
    schema: &str,
    options: &[tidb_ast::ColumnOption],
) -> Result<(), DdlPlanError> {
    if tidb_ddl_bdr::is_add_column_denied(role_of(stored), options)
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
    if tidb_ddl_bdr::is_modify_column_denied(
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
