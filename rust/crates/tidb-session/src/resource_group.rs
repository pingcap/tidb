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

//! Go's resource-group DDL: `DDLExec.execute{Create,Alter,Drop}ResourceGroup`
//! and `executor.{Add,Alter,Drop}ResourceGroup` (`ddl/executor.go`) with
//! `buildResourceGroup` and `checkResourceGroupValidation`
//! (`ddl/resource_group.go`, `ddl/resourcegroup/group.go`). The group lives
//! in the catalog as Go's meta keeps it; this tier has no PD resource
//! manager to register it with.

use tidb_ast::{
    AlterResourceGroupStmt, CreateResourceGroupStmt, DropResourceGroupStmt, ResourceGroupBurstable,
    ResourceGroupOption, ResourceGroupPriority, ResourceGroupRate,
};
use tidb_executor::{DriverError, MysqlError};
use tidb_model::{
    ActionType, ResourceGroupInfo, ResourceGroupSettings, DEFAULT_RESOURCE_GROUP_NAME,
};

use crate::{Session, StmtOutput};

/// Go `resourcegroup.MaxGroupNameLength`.
const MAX_GROUP_NAME_LENGTH: usize = 32;

/// Go `unlimitedRURate`.
const UNLIMITED_RU_RATE: u64 = i32::MAX as u64;

/// Go's `errors.New` messages, which reach the client as 1105.
fn plain_error(message: &str) -> DriverError {
    DriverError::Mysql(MysqlError::unknown(message))
}

/// Go `infoschema.ErrResourceGroupExists` (8248).
fn resource_group_exists(name: &str) -> DriverError {
    DriverError::Mysql(MysqlError::new(
        tidb_error::tidb::errcode::ErrResourceGroupExists,
        format!("Resource group '{name}' already exists"),
    ))
}

/// Go `infoschema.ErrResourceGroupNotExists` (8249).
fn resource_group_not_exists(name: &str) -> DriverError {
    DriverError::Mysql(MysqlError::new(
        tidb_error::tidb::errcode::ErrResourceGroupNotExists,
        format!("Unknown resource group '{name}'"),
    ))
}

/// Go `DDLExec`'s gate: `infoschema.ErrResourceGroupSupportDisabled` (8250)
/// unless `tidb_enable_resource_control` is on.
pub(crate) fn check_resource_control_enabled() -> Result<(), DriverError> {
    if tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    Err(DriverError::Mysql(MysqlError::new(
        tidb_error::tidb::errcode::ErrResourceGroupSupportDisabled,
        tidb_error::tidb::errname::ErrResourceGroupSupportDisabled.raw,
    )))
}

/// Go `buildResourceGroup`: `options` applied over a copy of `group` by
/// `SetDirectResourceGroupSettings`, then `Adjust`.
fn build_resource_group(
    group: &ResourceGroupInfo,
    options: &[ResourceGroupOption],
) -> Result<ResourceGroupInfo, DriverError> {
    let mut group = group.clone();
    let settings = group
        .settings
        .get_or_insert_with(|| Box::new(ResourceGroupSettings::new()));
    for option in options {
        match option {
            ResourceGroupOption::RuPerSec(ResourceGroupRate::Unlimited) => {
                settings.ru_rate = UNLIMITED_RU_RATE;
            }
            ResourceGroupOption::RuPerSec(ResourceGroupRate::Limited(rate)) => {
                settings.ru_rate = *rate;
            }
            ResourceGroupOption::Priority(priority) => {
                settings.priority = match priority {
                    ResourceGroupPriority::Low => tidb_ast::LOW_PRIORITY_VALUE,
                    ResourceGroupPriority::Medium => tidb_ast::MEDIUM_PRIORITY_VALUE,
                    ResourceGroupPriority::High => tidb_ast::HIGH_PRIORITY_VALUE,
                };
            }
            ResourceGroupOption::Burstable(burstable) => {
                settings.burst_limit = match burstable {
                    ResourceGroupBurstable::Unlimited => -1,
                    ResourceGroupBurstable::Moderated => -2,
                    ResourceGroupBurstable::Off => 0,
                };
            }
            ResourceGroupOption::QueryLimit(_) => return Err(DriverError::unsupported(
                "resource group QUERY_LIMIT (SetDirectResourceGroupRunawayOption) is not ported",
            )),
            ResourceGroupOption::Background(_) => {
                if group.name.lowercase() != DEFAULT_RESOURCE_GROUP_NAME {
                    return Err(plain_error(
                        "unsupported operation. Currently, only the default resource group \
                         support change background settings",
                    ));
                }
                return Err(DriverError::unsupported(
                    "resource group BACKGROUND (SetDirectResourceGroupBackgroundOption) is not ported",
                ));
            }
        }
    }
    settings.adjust();
    Ok(group)
}

/// Go `checkResourceGroupValidation`: `resourcegroup.NewGroupFromOptions`'
/// refusals for the options this tier builds.
fn check_resource_group_validation(group: &ResourceGroupInfo) -> Result<(), DriverError> {
    let Some(settings) = &group.settings else {
        return Err(plain_error("invalid group settings"));
    };
    if group.name.lowercase().len() > MAX_GROUP_NAME_LENGTH {
        return Err(plain_error("resource group name too long"));
    }
    if settings.ru_rate > 0 {
        if !settings.cpu_limiter.is_empty()
            || !settings.io_read_bandwidth.is_empty()
            || !settings.io_write_bandwidth.is_empty()
        {
            return Err(plain_error(
                "cannot set RU mode and Raw mode options at the same time",
            ));
        }
        return Ok(());
    }
    Err(plain_error("unknown resource group mode"))
}

impl Session {
    /// Go `DDLExec.executeCreateResourceGroup` and `executor.AddResourceGroup`.
    pub(crate) fn execute_create_resource_group(
        &mut self,
        stmt: &CreateResourceGroupStmt,
    ) -> Result<StmtOutput, DriverError> {
        check_resource_control_enabled()?;
        let fresh = ResourceGroupInfo {
            settings: Some(Box::new(ResourceGroupSettings::new())),
            name: tidb_ast::CiString::new(&stmt.name),
            ..ResourceGroupInfo::default()
        };
        let group = build_resource_group(&fresh, &stmt.options)?;
        let exists = self.lock_catalog()?.resource_group(&stmt.name).is_some();
        if exists {
            let error = resource_group_exists(&stmt.name);
            if stmt.if_not_exists {
                self.append_suppressed(error);
                return Ok(StmtOutput::Done(false));
            }
            return Err(error);
        }
        check_resource_group_validation(&group)?;
        self.submit_resource_group_job(ActionType::ACTION_CREATE_RESOURCE_GROUP, group)
    }

    /// Go `DDLExec.executeAlterResourceGroup` and `executor.AlterResourceGroup`.
    pub(crate) fn execute_alter_resource_group(
        &mut self,
        stmt: &AlterResourceGroupStmt,
    ) -> Result<StmtOutput, DriverError> {
        check_resource_control_enabled()?;
        let existing = self.lock_catalog()?.resource_group(&stmt.name).cloned();
        let Some(existing) = existing else {
            let error = resource_group_not_exists(&stmt.name);
            if stmt.if_exists {
                self.append_suppressed(error);
                return Ok(StmtOutput::Done(false));
            }
            return Err(error);
        };
        let group = build_resource_group(&existing, &stmt.options)?;
        check_resource_group_validation(&group)?;
        self.submit_resource_group_job(ActionType::ACTION_ALTER_RESOURCE_GROUP, group)
    }

    /// Go `DDLExec.executeDropResourceGroup` and `executor.DropResourceGroup`.
    pub(crate) fn execute_drop_resource_group(
        &mut self,
        stmt: &DropResourceGroupStmt,
    ) -> Result<StmtOutput, DriverError> {
        check_resource_control_enabled()?;
        if stmt.name.to_lowercase() == DEFAULT_RESOURCE_GROUP_NAME {
            return Err(plain_error("can't drop reserved resource group"));
        }
        let existing = self.lock_catalog()?.resource_group(&stmt.name).cloned();
        let Some(existing) = existing else {
            let error = resource_group_not_exists(&stmt.name);
            if stmt.if_exists {
                self.append_suppressed(error);
                return Ok(StmtOutput::Done(false));
            }
            return Err(error);
        };
        // Go's MatchUserResourceGroupName refusal needs an account bound to
        // the group, which this tier's ALTER USER cannot make yet.
        self.submit_resource_group_job(ActionType::ACTION_DROP_RESOURCE_GROUP, existing)
    }

    /// The resource-group job: BDR admission (`doDDLJob2`) and the meta edit
    /// `onCreateResourceGroup`/`onAlterResourceGroup`/`onDropResourceGroup`
    /// makes.
    fn submit_resource_group_job(
        &mut self,
        action: ActionType,
        group: ResourceGroupInfo,
    ) -> Result<StmtOutput, DriverError> {
        let cdc_write_source = self.ddl_cdc_write_source();
        self.with_catalog_mut(|catalog| {
            tidb_executor::ddl::admit_bdr_job(
                catalog,
                cdc_write_source,
                group.name.lowercase(),
                action,
            )?;
            if action == ActionType::ACTION_DROP_RESOURCE_GROUP {
                catalog.drop_resource_group(group.name.lowercase());
            } else {
                catalog.put_resource_group(group);
            }
            Ok(())
        })?;
        Ok(StmtOutput::Done(false))
    }
}
