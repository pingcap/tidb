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
    AlterResourceGroupStmt, CreateResourceGroupStmt, DropResourceGroupStmt,
    ResourceGroupBackgroundOption, ResourceGroupBurstable, ResourceGroupOption,
    ResourceGroupPriority, ResourceGroupRate, ResourceGroupRunawayAction as AstRunawayAction,
    ResourceGroupRunawayOption, ResourceGroupRunawayRule,
};
use tidb_config_configtypes::parse_go_duration;
use tidb_driver_error::DriverError;
use tidb_terror::MysqlError;
use tidb_hack::GoToLower;
use tidb_model::{
    ResourceGroupBackgroundSettings, ResourceGroupInfo,
    ResourceGroupRunawayAction as RunawayAction, ResourceGroupRunawaySettings,
    ResourceGroupSettings, ResourceGroupShared, DEFAULT_RESOURCE_GROUP_NAME,
};

use crate::Session;

/// Go `resourcegroup.MaxGroupNameLength`.
const MAX_GROUP_NAME_LENGTH: usize = 32;

/// Go `unlimitedRURate`.
const UNLIMITED_RU_RATE: u64 = i32::MAX as u64;

/// Go's `errors.New` messages, which reach the client as 1105.
fn plain_error(message: &str) -> DriverError {
    DriverError::Mysql(MysqlError::unknown(message))
}

/// Go `DDLExec`'s gate: `infoschema.ErrResourceGroupSupportDisabled` (8250)
/// unless `tidb_enable_resource_control` is on.
pub(crate) fn check_resource_control_enabled() -> Result<(), DriverError> {
    if tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    Err(DriverError::Mysql(MysqlError::from_terror(
        &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_SUPPORT_DISABLED,
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
            ResourceGroupOption::QueryLimit(options) => {
                settings.runaway = if options.is_empty() {
                    None
                } else {
                    let mut runaway = ResourceGroupRunawaySettings::default();
                    for option in options {
                        set_direct_resource_group_runaway_option(&mut runaway, option)?;
                    }
                    Some(ResourceGroupShared::new(runaway))
                };
            }
            ResourceGroupOption::Background(options) => {
                if group.name.lowercase() != DEFAULT_RESOURCE_GROUP_NAME {
                    return Err(plain_error(
                        "unsupported operation. Currently, only the default resource group \
                         support change background settings",
                    ));
                }
                // Go sets Background to nil for an empty list and then to a
                // fresh object regardless.
                let mut background = ResourceGroupBackgroundSettings::default();
                for option in options {
                    set_direct_resource_group_background_option(&mut background, option)?;
                }
                settings.background = Some(ResourceGroupShared::new(background));
            }
        }
    }
    settings.adjust();
    Ok(group)
}

/// Go `SetDirectResourceGroupRunawayOption`.
fn set_direct_resource_group_runaway_option(
    settings: &mut ResourceGroupRunawaySettings,
    option: &ResourceGroupRunawayOption,
) -> Result<(), DriverError> {
    match option {
        ResourceGroupRunawayOption::Rule(ResourceGroupRunawayRule::ExecElapsed(text)) => {
            let nanos = parse_go_duration(text).map_err(|error| plain_error(&error))?;
            // Go `uint64(dur.Milliseconds())`.
            settings.exec_elapsed_time_ms = (nanos / 1_000_000) as u64;
        }
        ResourceGroupRunawayOption::Rule(ResourceGroupRunawayRule::ProcessedKeys(keys)) => {
            settings.processed_keys = *keys;
        }
        ResourceGroupRunawayOption::Rule(ResourceGroupRunawayRule::RequestUnit(units)) => {
            settings.request_unit = *units;
        }
        ResourceGroupRunawayOption::Action(action) => {
            let (action, switch_group) = match action {
                AstRunawayAction::DryRun => (RunawayAction::DRY_RUN, String::new()),
                AstRunawayAction::Cooldown => (RunawayAction::COOLDOWN, String::new()),
                AstRunawayAction::Kill => (RunawayAction::KILL, String::new()),
                AstRunawayAction::SwitchGroup(name) => (RunawayAction::SWITCH_GROUP, name.clone()),
            };
            settings.action = action;
            settings.switch_group_name = switch_group;
        }
        ResourceGroupRunawayOption::Watch(watch) => {
            settings.watch_type = watch.watch_type.into();
            settings.watch_duration_ms = match watch.duration.as_deref() {
                Some(text) if !text.is_empty() => {
                    parse_go_duration(text).map_err(|error| plain_error(&error))? / 1_000_000
                }
                _ => 0,
            };
        }
    }
    Ok(())
}

/// Go `kvutil.ExplicitTypeList`.
const EXPLICIT_TYPE_LIST: [&str; 8] = [
    "",
    "lightning",
    "br",
    "dumpling",
    "background",
    "ddl",
    "stats",
    "import",
];

/// Go `SetDirectResourceGroupBackgroundOption` with `parseBackgroundJobTypes`.
fn set_direct_resource_group_background_option(
    settings: &mut ResourceGroupBackgroundSettings,
    option: &ResourceGroupBackgroundOption,
) -> Result<(), DriverError> {
    match option {
        ResourceGroupBackgroundOption::TaskTypes(text) => {
            let mut job_types = Vec::new();
            for segment in text.split(',').filter(|_| !text.is_empty()) {
                let job_type = segment.trim().to_lowercase();
                if job_type.is_empty() {
                    continue;
                }
                if !EXPLICIT_TYPE_LIST.contains(&job_type.as_str()) {
                    return Err(DriverError::Mysql(MysqlError::from_terror(&tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME.generate_with_stack_by_args(&[job_type.into()]))));
                }
                job_types.push(job_type);
            }
            settings.job_types = Some(job_types);
        }
        ResourceGroupBackgroundOption::UtilizationLimit(limit) => {
            if *limit == 0 || *limit > 100 {
                return Err(plain_error(
                    "invalid background resource utilization limit, the valid range is (0, 100]",
                ));
            }
            settings.resource_util_limit = *limit;
        }
    }
    Ok(())
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
    if let Some(runaway) = &settings.runaway {
        let runaway = runaway.read();
        if runaway.exec_elapsed_time_ms == 0
            && runaway.processed_keys == 0
            && runaway.request_unit == 0
        {
            return Err(plain_error(
                "please set at least one field(exec_elapsed_time_ms, processed_keys, ru)",
            ));
        }
        if runaway.action == RunawayAction::NONE {
            return Err(plain_error("unknown resource group runaway action"));
        }
        if runaway.action == RunawayAction::SWITCH_GROUP && runaway.switch_group_name.is_empty() {
            return Err(plain_error(
                "unknown resource group runaway switch group name",
            ));
        }
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
    /// Go `executeCreateUser`/`executeAlterUser`'s RESOURCE GROUP arm: the
    /// lowercase name the account's `user_attributes` records under
    /// `resource_group`, after the feature gate, the role refusal
    /// (`ErrResourceGroupInvalidForRole`, 8257) and the existence check
    /// (`default` and the empty name always pass).
    pub(crate) fn user_resource_group_attribute(
        &self,
        name: &str,
        is_role: bool,
    ) -> Result<String, DriverError> {
        check_resource_control_enabled()?;
        if is_role {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE,
            )));
        }
        let lower = name.go_to_lower();
        if lower != DEFAULT_RESOURCE_GROUP_NAME
            && !lower.is_empty()
            && self.lock_catalog()?.resource_group(&lower).is_none()
        {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_NOT_EXISTS
                    .generate_with_stack_by_args(&[(&lower).into()]),
            )));
        }
        Ok(lower)
    }

    /// Go `executor.AddResourceGroup` up to its job: the group to create, or
    /// `None` where `IF NOT EXISTS` filed the 8248 as a note.
    fn admit_create_resource_group(
        &mut self,
        stmt: &CreateResourceGroupStmt,
    ) -> Result<Option<ResourceGroupInfo>, DriverError> {
        check_resource_control_enabled()?;
        let fresh = ResourceGroupInfo {
            settings: Some(Box::new(ResourceGroupSettings::new())),
            name: tidb_ast::CiString::new(&stmt.name),
            ..ResourceGroupInfo::default()
        };
        let group = build_resource_group(&fresh, &stmt.options)?;
        let exists = self.lock_catalog()?.resource_group(&stmt.name).is_some();
        if exists {
            let error = DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_EXISTS
                    .generate_with_stack_by_args(&[(&stmt.name).into()]),
            ));
            if stmt.if_not_exists {
                self.append_suppressed(error);
                return Ok(None);
            }
            return Err(error);
        }
        check_resource_group_validation(&group)?;
        Ok(Some(group))
    }

    /// Go `executor.AlterResourceGroup` up to its job.
    fn admit_alter_resource_group(
        &mut self,
        stmt: &AlterResourceGroupStmt,
    ) -> Result<Option<ResourceGroupInfo>, DriverError> {
        check_resource_control_enabled()?;
        let existing = self.lock_catalog()?.resource_group(&stmt.name).cloned();
        let Some(existing) = existing else {
            let error = DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_NOT_EXISTS
                    .generate_with_stack_by_args(&[(&stmt.name).into()]),
            ));
            if stmt.if_exists {
                self.append_suppressed(error);
                return Ok(None);
            }
            return Err(error);
        };
        let group = build_resource_group(&existing, &stmt.options)?;
        check_resource_group_validation(&group)?;
        Ok(Some(group))
    }

    /// Go `executor.DropResourceGroup` up to its job.
    fn admit_drop_resource_group(
        &mut self,
        stmt: &DropResourceGroupStmt,
    ) -> Result<Option<ResourceGroupInfo>, DriverError> {
        check_resource_control_enabled()?;
        if stmt.name.go_to_lower() == DEFAULT_RESOURCE_GROUP_NAME {
            return Err(plain_error("can't drop reserved resource group"));
        }
        let existing = self.lock_catalog()?.resource_group(&stmt.name).cloned();
        let Some(existing) = existing else {
            let error = DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_NOT_EXISTS
                    .generate_with_stack_by_args(&[(&stmt.name).into()]),
            ));
            if stmt.if_exists {
                self.append_suppressed(error);
                return Ok(None);
            }
            return Err(error);
        };
        // Go `MatchUserResourceGroupName`: an account bound to the group
        // keeps it.
        if self.user_table_present() {
            let rows = self.user_table_rows(&format!(
                "SELECT user FROM mysql.user WHERE json_extract(user_attributes, \
                 '$.resource_group') = {} LIMIT 1",
                crate::user_table::sql_str(&stmt.name)
            ))?;
            if let Some(row) = rows.first() {
                let user = row
                    .first()
                    .map(crate::user_table::datum_text)
                    .unwrap_or_default();
                return Err(plain_error(&format!(
                    "user [{user}] depends on the resource group to drop"
                )));
            }
        }
        Ok(Some(existing))
    }

    /// The cluster DDL for a resource-group statement: Go's executor checks
    /// against this session's schema, then the statement the DDL job runs.
    /// `None` is an `IF [NOT] EXISTS` that filed its note and submits no job.
    pub fn cluster_resource_group_statement(
        &mut self,
        ddl: &tidb_ast::DdlStmt,
    ) -> Result<Option<tidb_ddl::cluster_ddl::DdlStatement>, DriverError> {
        use tidb_ddl::cluster_ddl::DdlStatement;
        Ok(match ddl {
            tidb_ast::DdlStmt::CreateResourceGroup(create) => self
                .admit_create_resource_group(create)?
                .map(|group| DdlStatement::CreateResourceGroup {
                    group: Box::new(group),
                }),
            tidb_ast::DdlStmt::AlterResourceGroup(alter) => self
                .admit_alter_resource_group(alter)?
                .map(|group| DdlStatement::AlterResourceGroup {
                    group: Box::new(group),
                }),
            tidb_ast::DdlStmt::DropResourceGroup(drop) => self
                .admit_drop_resource_group(drop)?
                .map(|group| DdlStatement::DropResourceGroup {
                    group: Box::new(group),
                }),
            _ => None,
        })
    }
}
