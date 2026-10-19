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

//! Resource groups as Go runs them: the session's executor checks the
//! statement (`AddResourceGroup`, `AlterResourceGroup`, `DropResourceGroup`)
//! and queues a job; the owner's `onCreateResourceGroup`,
//! `onAlterResourceGroup` and `onDropResourceGroup` change the meta record
//! and tell the resource manager in the same step.

use super::add_index::{action_step, AddIndexSubmission};
use super::*;

/// Go's executor halves up to `doDDLJob2`: the session already checked the
/// statement and resolved the group. `None` for any other statement.
pub fn prepare_resource_group_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &tidb_executor::StmtContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (action, group, schema_id, id_allocated) = match statement {
        DdlStatement::CreateResourceGroup { group } => {
            let mut info = (**group).clone();
            info.id = 0;
            info.state = SchemaState::NONE;
            // `assignGIDsForJobs` draws the group's id.
            (ActionType::ACTION_CREATE_RESOURCE_GROUP, info, 0, false)
        }
        DdlStatement::AlterResourceGroup { group } => (
            ActionType::ACTION_ALTER_RESOURCE_GROUP,
            (**group).clone(),
            group.id,
            true,
        ),
        DdlStatement::DropResourceGroup { group } => (
            ActionType::ACTION_DROP_RESOURCE_GROUP,
            tidb_model::ResourceGroupInfo {
                name: group.name.clone(),
                ..tidb_model::ResourceGroupInfo::default()
            },
            group.id,
            true,
        ),
        _ => return Ok(None),
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let mut job = session_job(job_context, action);
    job.schema_id = schema_id;
    job.schema_name = group.name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
        resource_group: group.name.lowercase().to_owned().into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }]);
    let args = tidb_model::ResourceGroupArgs {
        resource_group_info: GoField::new(Some(GoShared::new(group))),
    };
    let built = AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::ResourceGroup(Some(GoShared::new(args))),
            id_allocated,
        }),
        Vec::new(),
    );
    submit_built_job(
        snapshot,
        &catalog,
        Some(built),
        start_ts,
        upgrading,
        min_job_id,
    )
}

fn group_args(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<tidb_model::ResourceGroupInfo, DdlPlanError> {
    let args = tidb_model::get_resource_group_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("resource group job has nil args".to_owned()),
            )
        })?;
    let info = args.read().resource_group_info.get();
    info.map(|info| info.read().clone()).ok_or_else(|| {
        cancel_ddl_job(
            active,
            DdlPlanError::Encode("resource group job has no group".to_owned()),
        )
    })
}

/// Go `checkResourceGroupExist`.
fn stored_group<'a>(
    catalog: &'a ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    group_id: i64,
) -> Result<&'a tidb_model::ResourceGroupInfo, DdlPlanError> {
    stored_resource_group(catalog, group_id).map_err(|error| cancel_ddl_job(active, error))
}

/// One step publishing a group change under a new version.
fn group_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    mutation: BufferMutation,
    update: ResourceGroupUpdate,
    finished_state: SchemaState,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let schema_version = catalog.schema_version + 1;
    let diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: active.job.schema_id,
        ..SchemaDiff::default()
    };
    let mutations = vec![
        mutation,
        BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?,
        BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?,
    ];
    active.job.last_schema_version = schema_version;
    active
        .job
        .finish_db_job(JobState::DONE, finished_state, schema_version, None);
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.resource_group_update = Some(update);
    Ok(action_step(write))
}

/// Go `onCreateResourceGroup`.
pub(super) fn plan_persisted_create_resource_group_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let mut group = group_args(active)?;
    let proto = resource_group_proto(&group).map_err(|error| cancel_ddl_job(active, error))?;
    group.state = SchemaState::PUBLIC;
    let mutation = BufferMutation::set(
        key::resource_group_kv_key(group.id),
        serialize_resource_group(&group)?,
    )?;
    active.job.schema_id = group.id;
    group_step(
        catalog,
        active,
        mutation,
        ResourceGroupUpdate::Add(Box::new(proto)),
        SchemaState::PUBLIC,
    )
}

/// Go `onAlterResourceGroup`.
pub(super) fn plan_persisted_alter_resource_group_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let altered = group_args(active)?;
    let proto = resource_group_proto(&altered).map_err(|error| cancel_ddl_job(active, error))?;
    let mut group = stored_group(catalog, active, altered.id)?.clone();
    group.settings = altered.settings;
    let mutation = BufferMutation::set(
        key::resource_group_kv_key(group.id),
        serialize_resource_group(&group)?,
    )?;
    group_step(
        catalog,
        active,
        mutation,
        ResourceGroupUpdate::Modify(Box::new(proto)),
        SchemaState::PUBLIC,
    )
}

/// Go `onDropResourceGroup`: public -> none in one step.
pub(super) fn plan_persisted_drop_resource_group_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let group_id = active.job.schema_id;
    let group = stored_group(catalog, active, group_id)?.clone();
    if group.state != SchemaState::PUBLIC {
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                FormatArg::from("resource_group"),
                FormatArg::new(
                    group.state.to_string(),
                    group.state.0.to_string(),
                    "model.SchemaState",
                ),
            ]),
        ));
    }
    let mutation = BufferMutation::delete(key::resource_group_kv_key(group.id))?;
    group_step(
        catalog,
        active,
        mutation,
        ResourceGroupUpdate::Delete(group.name.lowercase().to_owned()),
        SchemaState::NONE,
    )
}
