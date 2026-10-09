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

//! Go's BDR admission over this synchronous DDL owner.
//!
//! Go refuses a DDL under a BDR role in two places: `jobsubmit.SubmitBatch`
//! asks `bdr.IsDenied` of every job it submits (each sub-job of a
//! multi-schema change), and the executor's ADD COLUMN and MODIFY COLUMN
//! builders ask `IsAddColumnDenied` / `IsModifyColumnDenied` of the column
//! they built. This owner has no job queue, so each DDL calls [`admit`] with
//! the jobs Go would submit, after the statement's own validation and before
//! anything changes -- the point a Go job reaches the submitter.

use tidb_ast::BdrRole;
use tidb_model::ActionType;

use crate::{Catalog, DriverError};

/// One job as Go's submitter sees it: its action, and for an ADD INDEX /
/// ADD PRIMARY KEY job whether the first index it adds is unique.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SubmittedJob {
    pub(crate) action: ActionType,
    pub(crate) first_index_unique: Option<bool>,
}

impl SubmittedJob {
    pub(crate) const fn new(action: ActionType) -> Self {
        Self {
            action,
            first_index_unique: None,
        }
    }

    pub(crate) const fn add_index(action: ActionType, unique: bool) -> Self {
        Self {
            action,
            first_index_unique: Some(unique),
        }
    }
}

/// The stored role (Go `meta.Mutator.GetBDRRole`) as `bdr`'s argument;
/// `None` is Go's `BDRRoleNone`.
fn role_of(stored: &str) -> Option<BdrRole> {
    match stored {
        "primary" => Some(BdrRole::Primary),
        "secondary" => Some(BdrRole::Secondary),
        _ => None,
    }
}

/// `dbterror.ErrBDRRestrictedDDL` (8263).
fn restricted(stored: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::tidb::errcode::ErrBDRRestrictedDDL,
        message: format!(
            "The operation is not allowed while the bdr role of this cluster is set to {stored}."
        ),
    }
}

/// Go `SubmitBatch`'s admission for one statement's jobs, captured before
/// the statement takes the table it changes: a job not from TiCDC
/// (`job.CDCWriteSource == 0`), under a set role, on a schema that is not a
/// system schema, is refused when `bdr.IsDenied` says so. A multi-schema
/// change is refused when any of its sub-jobs is.
pub(crate) struct Admission {
    stored: String,
    applies: bool,
}

impl Admission {
    pub(crate) fn new(catalog: &Catalog, cdc_write_source: u64, schema: &str) -> Self {
        let stored = catalog.bdr_role();
        let applies = cdc_write_source == 0
            && role_of(&stored).is_some()
            && !tidb_util::filter::is_system_schema(&schema.to_lowercase());
        Self { stored, applies }
    }

    pub(crate) fn admit(&self, jobs: &[SubmittedJob]) -> Result<(), DriverError> {
        let role = role_of(&self.stored);
        if self.applies
            && jobs.iter().any(|job| {
                tidb_model::ddl_bdr::is_action_denied(role, job.action, job.first_index_unique)
            })
        {
            return Err(restricted(&self.stored));
        }
        Ok(())
    }
}

/// Go `SubmitBatch`'s BDR admission of one job without arguments, for a
/// DDL the session runs itself (CREATE / DROP DATABASE).
pub fn admit_job(
    catalog: &Catalog,
    cdc_write_source: u64,
    schema: &str,
    action: ActionType,
) -> Result<(), DriverError> {
    admit(
        catalog,
        cdc_write_source,
        schema,
        &[SubmittedJob::new(action)],
    )
}

/// [`Admission`] for a statement that still holds the catalog.
pub(crate) fn admit(
    catalog: &Catalog,
    cdc_write_source: u64,
    schema: &str,
    jobs: &[SubmittedJob],
) -> Result<(), DriverError> {
    Admission::new(catalog, cdc_write_source, schema).admit(jobs)
}

/// Go `AddColumn`'s `IsAddColumnDenied` check, after the column is built.
pub(crate) fn admit_add_column(
    catalog: &Catalog,
    schema: &str,
    options: &[tidb_ast::ColumnOption],
) -> Result<(), DriverError> {
    let stored = catalog.bdr_role();
    if tidb_model::ddl_bdr::is_add_column_denied(role_of(&stored), options)
        && !tidb_util::filter::is_system_schema(&schema.to_lowercase())
    {
        return Err(restricted(&stored));
    }
    Ok(())
}

/// Go `GetModifiableColumnJob`'s `IsModifyColumnDenied` check, after the
/// new column is built.
pub(crate) fn admit_modify_column(
    catalog: &Catalog,
    schema: &str,
    new_field_type: &tidb_datatype::FieldType,
    old_field_type: &tidb_datatype::FieldType,
    options: &[tidb_ast::ColumnOption],
) -> Result<(), DriverError> {
    let stored = catalog.bdr_role();
    if tidb_model::ddl_bdr::is_modify_column_denied(
        role_of(&stored),
        new_field_type,
        old_field_type,
        options,
    ) && !tidb_util::filter::is_system_schema(&schema.to_lowercase())
    {
        return Err(restricted(&stored));
    }
    Ok(())
}
