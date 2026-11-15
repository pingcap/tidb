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

/// Go `executor.CreateSchema`'s `dbInfo`: the name, the resolved charset and
/// collation, and the written placement policy by name.
pub fn build_db_info(
    name: &str,
    charset: &str,
    collate: &str,
    placement_policy: Option<&str>,
) -> DBInfo {
    DBInfo {
        name: CiString::new(name),
        charset: charset.to_owned(),
        collate: collate.to_owned(),
        placement_policy_ref: placement_policy.map(|policy| {
            GoShared::new(tidb_model::PolicyRefInfo {
                id: 0,
                name: CiString::new(policy),
            })
        }),
        ..DBInfo::default()
    }
}

/// Go `executor.CreateSchemaWithInfo`: admission and job construction.
pub fn create_schema_with_info<S: MetaSnapshot>(
    snapshot: &mut S,
    mut info: DBInfo,
    on_exist: crate::OnExist,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    if find_database(&catalog, info.name.original()).is_some() {
        // Go keeps the stack: the error may be seen as an error.
        let error = tidb_infoschema::infoschema_errors::ERR_DATABASE_EXISTS
            .gen_with_stack_by_args(&[info.name.original().into()]);
        return match on_exist {
            crate::OnExist::Ignore => Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("database `{}` already exists", info.name.original()),
                warnings: vec![crate::real_tikv_ddl::ddl_note(error)],
            })),
            // Go: "can we implement MariaDB's CREATE OR REPLACE SCHEMA?"
            crate::OnExist::Error | crate::OnExist::Replace => Err(DdlPlanError::from(error)),
        };
    }
    // Go `checkTooLongSchema`.
    tidb_ddl_build::check_too_long_identifier(info.name.lowercase())
        .map_err(DdlPlanError::from_mysql_error)?;
    tidb_ddl_build::table_info_build::check_charset_and_collation(&info.charset, &info.collate)
        .map_err(DdlPlanError::Admission)?;
    let mut warnings = Vec::new();
    handle_database_placement(snapshot, job_context, &mut info, &mut warnings)?;
    let placement_policy_ref = info.placement_policy_ref.clone();
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
    let mut specs = [tidb_ddl_jobsubmit::JobSpec {
        job,
        args: JobArgsValue::CreateSchema(Some(args)),
        // `assignGIDsForJobs` draws the schema id.
        id_allocated: false,
    }];
    tidb_ddl_jobsubmit::prepare_submit_batch(snapshot, &mut specs, start_ts, upgrading)?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), warnings)))
}
