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

//! CREATE DATABASE as Go runs it: `executor.CreateSchemaWithInfo` queues a
//! job whose schema id `assignGIDsForJobs` draws, and the owner's
//! `onCreateSchema` publishes the database in one step.

use super::add_index::AddIndexSubmission;
use super::*;

/// Go `executor.CreateSchema` -> `CreateSchemaWithInfo` up to `doDDLJob2`.
/// `None` for any other statement.
pub fn prepare_create_schema_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::CreateDatabase {
        name,
        if_not_exists,
        charset,
        collate,
        placement_policy,
    } = statement
    else {
        return Ok(None);
    };
    let catalog = load_cluster_catalog(snapshot)?;
    if let Some(existing) = find_database(&catalog, name) {
        if *if_not_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!(
                    "database `{}` already exists",
                    existing.info.name.original()
                ),
                warnings: vec![(
                    DdlWarningLevel::Note,
                    1007,
                    format!("Can't create database '{name}'; database exists"),
                )],
            }));
        }
        return Err(DdlPlanError::DatabaseExists(name.clone()));
    }
    // Go `checkTooLongSchema`.
    tidb_executor::ddl::check_too_long_identifier(&name.go_to_lower()).map_err(|error| {
        let error = error.to_mysql_error();
        coded_refusal(error.code, error.message)
    })?;
    // Go `handleDatabasePlacement`.
    let placement_policy_ref = match placement_policy {
        Some(policy) => check_and_normalize_placement_policy(snapshot, policy)?,
        None => None,
    };
    let info = DBInfo {
        name: CiString::new(name.clone()),
        charset: charset.clone(),
        collate: collate.clone(),
        placement_policy_ref: placement_policy_ref.clone(),
        ..DBInfo::default()
    };
    let mut job = session_job(job_context, ActionType::ACTION_CREATE_SCHEMA);
    job.schema_name = info.name.lowercase().to_owned().into();
    let mut involving = vec![tidb_model::InvolvingSchemaInfo {
        database: info.name.lowercase().to_owned().into(),
        table: tidb_model::job::INVOLVING_ALL.into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }];
    if let Some(policy) = &placement_policy_ref {
        involving.push(tidb_model::InvolvingSchemaInfo {
            policy: policy.read().name.lowercase().to_owned().into(),
            mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
            ..tidb_model::InvolvingSchemaInfo::default()
        });
    }
    job.involving_schema_info = GoSharedSlice::from_vec(involving);
    let args = GoShared::new(tidb_model::CreateSchemaArgs {
        db_info: GoField::new(Some(GoShared::new(info))),
    });
    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args: JobArgsValue::CreateSchema(Some(args)),
        // `assignGIDsForJobs` draws the schema id.
        id_allocated: false,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}
