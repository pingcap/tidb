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

//! Placement policies as Go runs them: the executor checks the statement and
//! queues an `ActionCreatePlacementPolicy` (with an id from the policies' own
//! sequence, `genPlacementPolicyID`), `ActionAlterPlacementPolicy` or
//! `ActionDropPlacementPolicy` job. The owner's `onCreatePlacementPolicy`
//! creates (or, OR REPLACE, updates) it, `onAlterPlacementPolicy` updates it
//! and resends the bundles of every object that names it, and
//! `onDropPlacementPolicy` takes it through write-only and delete-only.

use super::add_index::{action_step, AddIndexSubmission};
use super::*;
use tidb_hack::GoCaseCompare;

/// Go `ErrPlacementPolicyNotExists`.
fn policy_not_exists(name: &str) -> DdlPlanError {
    coded_refusal(
        &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS,
        format!("Unknown placement policy '{name}'"),
    )
}

/// Go `checkPolicyValidation`: the settings must build a bundle.
fn check_policy_validation(settings: &tidb_model::PlacementSettings) -> Result<(), DdlPlanError> {
    tidb_placement::new_bundle_from_options(Some(settings))
        .map(|_| ())
        .map_err(|error| DdlPlanError::plain_refusal(format!("building placement rules: {error}")))
}

/// Go `CheckPlacementPolicyNotInUseFromInfoSchema`: no database, table or
/// partition may still name the policy.
fn check_policy_not_in_use(
    catalog: &ClusterCatalog,
    policy: &CiString,
) -> Result<(), DdlPlanError> {
    let named_by_database = catalog.databases.iter().any(|database| {
        database
            .info
            .placement_policy_ref
            .as_ref()
            .is_some_and(|reference| reference.read().name.lowercase() == policy.lowercase())
    });
    if named_by_database || policy_referenced(catalog, policy) {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_PLACEMENT_POLICY_IN_USE,
            format!("Placement policy '{}' is still in use", policy.original()),
        ));
    }
    Ok(())
}

/// Go's executor halves up to `doDDLJob2`. `None` for any other statement.
pub fn prepare_placement_policy_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let (action, policy_name, schema_id, args) = match statement {
        // Go `executor.CreatePlacementPolicyWithInfo`.
        DdlStatement::CreatePlacementPolicy {
            name,
            settings,
            if_not_exists,
            or_replace,
        } => {
            if name.go_lower_eq("default") {
                return Err(coded_refusal(
                    &tidb_error::catalog::infoschema::ERR_RESERVED_SYNTAX,
                    format!(
                        "The '{name}' syntax is reserved for purposes internal to the MySQL server"
                    ),
                ));
            }
            if find_policy(snapshot, name)?.is_some() && !*or_replace {
                if *if_not_exists {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!("placement policy `{name}` already exists"),
                        warnings: vec![crate::real_tikv_ddl::ddl_note(
                            &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_EXISTS,
                            format!("Placement policy '{name}' already exists"),
                        )],
                    }));
                }
                return Err(coded_refusal(
                    &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_EXISTS,
                    format!("Placement policy '{name}' already exists"),
                ));
            }
            check_policy_validation(settings)?;
            // The id comes from the policies' own sequence at submission.
            let policy = tidb_model::PolicyInfo {
                placement_settings: Some(GoShared::new(settings.clone())),
                id: 0,
                name: CiString::new(name.clone()),
                state: SchemaState::NONE,
            };
            (
                ActionType::ACTION_CREATE_PLACEMENT_POLICY,
                CiString::new(name.clone()),
                0,
                tidb_model::PlacementPolicyArgs {
                    policy: Some(GoShared::new(policy)),
                    replace_on_exist: *or_replace,
                    ..tidb_model::PlacementPolicyArgs::default()
                },
            )
        }
        // Go `executor.AlterPlacementPolicy`: a missing policy is 8239
        // whatever IF EXISTS says.
        DdlStatement::AlterPlacementPolicy { name, settings, .. } => {
            let Some(found) = find_policy(snapshot, name)? else {
                return Err(policy_not_exists(name));
            };
            check_policy_validation(settings)?;
            let policy = tidb_model::PolicyInfo {
                placement_settings: Some(GoShared::new(settings.clone())),
                id: 0,
                name: found.name.clone(),
                state: SchemaState::NONE,
            };
            (
                ActionType::ACTION_ALTER_PLACEMENT_POLICY,
                found.name.clone(),
                found.id,
                tidb_model::PlacementPolicyArgs {
                    policy: Some(GoShared::new(policy)),
                    policy_id: found.id,
                    ..tidb_model::PlacementPolicyArgs::default()
                },
            )
        }
        // Go `executor.DropPlacementPolicy`.
        DdlStatement::DropPlacementPolicy { name, if_exists } => {
            let Some(found) = find_policy(snapshot, name)? else {
                if *if_exists {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!("placement policy `{name}` does not exist"),
                        warnings: vec![crate::real_tikv_ddl::ddl_note(
                            &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS,
                            format!("Unknown placement policy '{name}'"),
                        )],
                    }));
                }
                return Err(policy_not_exists(name));
            };
            check_policy_not_in_use(&catalog, &found.name)?;
            (
                ActionType::ACTION_DROP_PLACEMENT_POLICY,
                CiString::new(name.clone()),
                found.id,
                tidb_model::PlacementPolicyArgs {
                    policy_name: CiString::new(name.clone()),
                    policy_id: found.id,
                    ..tidb_model::PlacementPolicyArgs::default()
                },
            )
        }
        _ => return Ok(None),
    };
    let mut job = session_job(job_context, action);
    job.schema_id = schema_id;
    job.schema_name = policy_name.lowercase().to_owned().into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
        policy: policy_name.lowercase().to_owned().into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }]);
    let built = AddIndexSubmission::Job(
        Box::new(crate::ddl_job_submit::JobSpec {
            job,
            args: JobArgsValue::PlacementPolicy(Some(GoShared::new(args))),
            id_allocated: true,
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

/// Go `genPlacementPolicyID`, set on a CREATE PLACEMENT POLICY job before
/// it is queued.
pub fn set_placement_policy_id(spec: &crate::ddl_job_submit::JobSpec, id: i64) {
    if let JobArgsValue::PlacementPolicy(Some(args)) = &spec.args {
        if let Some(policy) = &args.read().policy {
            policy.write().id = id;
        }
    }
}

fn policy_args(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<GoShared<tidb_model::PlacementPolicyArgs>, DdlPlanError> {
    tidb_model::get_placement_policy_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("placement policy job has nil args".to_owned()),
            )
        })
}

/// Go `checkPlacementPolicyExistAndCancelNonExistJob`.
fn stored_policy<S: MetaSnapshot>(
    snapshot: &mut S,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    policy_id: i64,
) -> Result<tidb_model::PolicyInfo, DdlPlanError> {
    let stored = snapshot
        .get(&key::policy_kv_key(policy_id))?
        .map(|bytes| value::parse_policy_info(&bytes))
        .transpose()
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    stored.ok_or_else(|| {
        cancel_ddl_job(
            active,
            coded_refusal(
                &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS,
                format!("Unknown placement policy '(Policy ID {policy_id})'"),
            ),
        )
    })
}

/// One step publishing a policy change under a new version (Go
/// `updateSchemaVersion` with the job's `SchemaID`).
fn policy_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    mut mutations: Vec<BufferMutation>,
    bundles: Vec<tidb_placement::Bundle>,
) -> Result<DdlWrite, DdlPlanError> {
    let schema_version = catalog.schema_version + 1;
    let diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: active.job.schema_id,
        ..SchemaDiff::default()
    };
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    active.job.last_schema_version = schema_version;
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.placement_bundles = bundles;
    Ok(write)
}

/// Go `updateExistPlacementPolicy`: the policy, and the bundles of every
/// object naming it.
fn update_exist_placement_policy(
    catalog: &ClusterCatalog,
    policy: &tidb_model::PolicyInfo,
) -> Result<(BufferMutation, Vec<tidb_placement::Bundle>), DdlPlanError> {
    let settings = policy
        .placement_settings
        .as_ref()
        .map(|settings| settings.read().clone())
        .unwrap_or_default();
    let bundles = rebuilt_bundles_for_policy(catalog, &policy.name, &settings)?;
    let mutation = BufferMutation::set(
        key::policy_kv_key(policy.id),
        value::serialize_policy_info(policy)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?;
    Ok((mutation, bundles))
}

/// Go `onCreatePlacementPolicy`.
pub(super) fn plan_persisted_create_placement_policy_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = policy_args(active)?;
    let (policy, or_replace) = {
        let args = args.read();
        let policy = args
            .policy
            .as_ref()
            .ok_or_else(|| DdlPlanError::Encode("CREATE POLICY has no policy".to_owned()))?
            .read()
            .clone();
        (policy, args.replace_on_exist)
    };
    let settings = policy
        .placement_settings
        .as_ref()
        .map(|settings| settings.read().clone())
        .unwrap_or_default();
    check_policy_validation(&settings).map_err(|error| cancel_ddl_job(active, error))?;
    let existing = find_policy(snapshot, policy.name.original())?;
    if let Some(existing) = existing {
        if !or_replace {
            return Err(cancel_ddl_job(
                active,
                coded_refusal(
                    &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_EXISTS,
                    format!(
                        "Placement policy '{}' already exists",
                        existing.name.original()
                    ),
                ),
            ));
        }
        let mut replacement = existing.clone();
        replacement.placement_settings = Some(GoShared::new(settings));
        let (mutation, bundles) = update_exist_placement_policy(catalog, &replacement)
            .map_err(|error| cancel_ddl_job(active, error))?;
        active.job.schema_id = replacement.id;
        let write = policy_step(catalog, active, vec![mutation], bundles)?;
        active.job.finish_db_job(
            JobState::DONE,
            SchemaState::PUBLIC,
            write.schema_version,
            None,
        );
        return Ok(action_step(write));
    }
    let mut created = policy;
    created.state = SchemaState::PUBLIC;
    let mutation = BufferMutation::set(
        key::policy_kv_key(created.id),
        value::serialize_policy_info(&created)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?;
    active.job.schema_id = created.id;
    let write = policy_step(catalog, active, vec![mutation], Vec::new())?;
    active.job.finish_db_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        write.schema_version,
        None,
    );
    Ok(action_step(write))
}

/// Go `onAlterPlacementPolicy`.
pub(super) fn plan_persisted_alter_placement_policy_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = policy_args(active)?;
    let (policy_id, settings) = {
        let args = args.read();
        let settings = args
            .policy
            .as_ref()
            .and_then(|policy| policy.read().placement_settings.clone())
            .map(|settings| settings.read().clone())
            .unwrap_or_default();
        (args.policy_id, settings)
    };
    let mut policy = stored_policy(snapshot, active, policy_id)?;
    // Go returns the validation error without cancelling: it is retried.
    check_policy_validation(&settings)?;
    policy.placement_settings = Some(GoShared::new(settings));
    let (mutation, bundles) = update_exist_placement_policy(catalog, &policy)
        .map_err(|error| cancel_ddl_job(active, error))?;
    let write = policy_step(catalog, active, vec![mutation], bundles)?;
    active.job.finish_db_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        write.schema_version,
        None,
    );
    Ok(action_step(write))
}

/// Go `onDropPlacementPolicy`.
pub(super) fn plan_persisted_drop_placement_policy_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let policy_id = policy_args(active)?.read().policy_id;
    let mut policy = stored_policy(snapshot, active, policy_id)?;
    // Go `checkPlacementPolicyNotInUse`.
    check_policy_not_in_use(catalog, &policy.name)
        .map_err(|error| cancel_ddl_job(active, error))?;
    let finished = policy.state == SchemaState::DELETE_ONLY;
    let mutation = match policy.state {
        SchemaState::PUBLIC | SchemaState::WRITE_ONLY => {
            policy.state = if policy.state == SchemaState::PUBLIC {
                SchemaState::WRITE_ONLY
            } else {
                SchemaState::DELETE_ONLY
            };
            BufferMutation::set(
                key::policy_kv_key(policy.id),
                value::serialize_policy_info(&policy)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?
        }
        SchemaState::DELETE_ONLY => BufferMutation::delete(key::policy_kv_key(policy.id))?,
        state => {
            return Err(DdlPlanError::from(
                tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                    FormatArg::from("policy"),
                    FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
                ]),
            ))
        }
    };
    let write = policy_step(catalog, active, vec![mutation], Vec::new())?;
    if finished {
        active.job.finish_db_job(
            JobState::DONE,
            SchemaState::NONE,
            write.schema_version,
            None,
        );
    } else {
        active.job.schema_state = policy.state;
    }
    Ok(action_step(write))
}
