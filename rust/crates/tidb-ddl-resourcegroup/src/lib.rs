// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete transcreation of pinned Go `pkg/ddl/resourcegroup`.

use tidb_model::{ResourceGroupRunawayAction, ResourceGroupRunawayWatch, ResourceGroupSettings};
use tikv_client::proto::resource_manager as rmpb;

/// Go `MaxGroupNameLength`.
pub const MAX_GROUP_NAME_LENGTH: usize = 32;

/// Package errors declared in Go `errors.go`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Go `ErrInvalidGroupSettings`.
    InvalidGroupSettings,
    /// Go `ErrTooLongResourceGroupName`.
    TooLongResourceGroupName,
    /// Go `ErrInvalidResourceGroupFormat`.
    InvalidResourceGroupFormat,
    /// Go `ErrInvalidResourceGroupDuplicatedMode`.
    InvalidResourceGroupDuplicatedMode,
    /// Go `ErrUnknownResourceGroupMode`.
    UnknownResourceGroupMode,
    /// Go `ErrDroppingInternalResourceGroup`.
    DroppingInternalResourceGroup,
    /// Go `ErrResourceGroupRunawayRuleIsEmpty`.
    ResourceGroupRunawayRuleIsEmpty,
    /// Go `ErrUnknownResourceGroupRunawayAction`.
    UnknownResourceGroupRunawayAction,
    /// Go `ErrUnknownResourceGroupRunawaySwitchGroupName`.
    UnknownResourceGroupRunawaySwitchGroupName,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidGroupSettings => "invalid group settings",
            Self::TooLongResourceGroupName => "resource group name too long",
            Self::InvalidResourceGroupFormat => "group settings with invalid format",
            Self::InvalidResourceGroupDuplicatedMode => {
                "cannot set RU mode and Raw mode options at the same time"
            }
            Self::UnknownResourceGroupMode => "unknown resource group mode",
            Self::DroppingInternalResourceGroup => "can't drop reserved resource group",
            Self::ResourceGroupRunawayRuleIsEmpty => {
                "please set at least one field(exec_elapsed_time_ms, processed_keys, ru)"
            }
            Self::UnknownResourceGroupRunawayAction => "unknown resource group runaway action",
            Self::UnknownResourceGroupRunawaySwitchGroupName => {
                "unknown resource group runaway switch group name"
            }
        })
    }
}

impl std::error::Error for Error {}

/// Go `NewGroupFromOptions`.
pub fn new_group_from_options(
    group_name: &str,
    options: Option<&ResourceGroupSettings>,
) -> Result<rmpb::ResourceGroup, Error> {
    let Some(options) = options else {
        return Err(Error::InvalidGroupSettings);
    };
    if group_name.len() > MAX_GROUP_NAME_LENGTH {
        return Err(Error::TooLongResourceGroupName);
    }

    let mut group = rmpb::ResourceGroup {
        name: group_name.to_owned(),
        priority: options.priority as u32,
        ..rmpb::ResourceGroup::default()
    };

    if let Some(runaway) = &options.runaway {
        let runaway = runaway.read();
        if runaway.exec_elapsed_time_ms == 0
            && runaway.processed_keys == 0
            && runaway.request_unit == 0
        {
            return Err(Error::ResourceGroupRunawayRuleIsEmpty);
        }
        if runaway.action == ResourceGroupRunawayAction::NONE {
            return Err(Error::UnknownResourceGroupRunawayAction);
        }
        if runaway.action == ResourceGroupRunawayAction::SWITCH_GROUP
            && runaway.switch_group_name.is_empty()
        {
            return Err(Error::UnknownResourceGroupRunawaySwitchGroupName);
        }

        let watch =
            (runaway.watch_type != ResourceGroupRunawayWatch::NONE).then(|| rmpb::RunawayWatch {
                r#type: runaway.watch_type.0,
                lasting_duration_ms: runaway.watch_duration_ms,
            });
        group.runaway_settings = Some(rmpb::RunawaySettings {
            rule: Some(rmpb::RunawayRule {
                exec_elapsed_time_ms: runaway.exec_elapsed_time_ms,
                processed_keys: runaway.processed_keys,
                request_unit: runaway.request_unit,
            }),
            action: runaway.action.0,
            watch,
            switch_group_name: runaway.switch_group_name.clone(),
        });
    }

    if let Some(background) = &options.background {
        let background = background.read();
        group.background_settings = Some(rmpb::BackgroundSettings {
            job_types: background.job_types.clone().unwrap_or_default(),
            utilization_limit: background.resource_util_limit,
        });
    }

    if options.ru_rate > 0 {
        group.mode = rmpb::GroupMode::RuMode as i32;
        group.r_u_settings = Some(rmpb::GroupRequestUnitSettings {
            r_u: Some(rmpb::TokenBucket {
                settings: Some(rmpb::TokenLimitSettings {
                    fill_rate: options.ru_rate,
                    burst_limit: options.burst_limit,
                    ..rmpb::TokenLimitSettings::default()
                }),
                ..rmpb::TokenBucket::default()
            }),
        });
        if !options.cpu_limiter.is_empty()
            || !options.io_read_bandwidth.is_empty()
            || !options.io_write_bandwidth.is_empty()
        {
            return Err(Error::InvalidResourceGroupDuplicatedMode);
        }
        return Ok(group);
    }

    Err(Error::UnknownResourceGroupMode)
}

#[cfg(test)]
mod error_literal_tests {
    use super::*;

    /// Pins every error Display text byte-for-byte against Go
    /// `pkg/ddl/resourcegroup/errors.go` so future sweeps cannot drift.
    #[test]
    fn error_display_texts_match_go_literals() {
        let literals: &[(Error, &str)] = &[
            (Error::InvalidGroupSettings, "invalid group settings"),
            (
                Error::TooLongResourceGroupName,
                "resource group name too long",
            ),
            (
                Error::InvalidResourceGroupFormat,
                "group settings with invalid format",
            ),
            (
                Error::InvalidResourceGroupDuplicatedMode,
                "cannot set RU mode and Raw mode options at the same time",
            ),
            (
                Error::UnknownResourceGroupMode,
                "unknown resource group mode",
            ),
            (
                Error::DroppingInternalResourceGroup,
                "can't drop reserved resource group",
            ),
            (
                Error::ResourceGroupRunawayRuleIsEmpty,
                "please set at least one field(exec_elapsed_time_ms, processed_keys, ru)",
            ),
            (
                Error::UnknownResourceGroupRunawayAction,
                "unknown resource group runaway action",
            ),
            (
                Error::UnknownResourceGroupRunawaySwitchGroupName,
                "unknown resource group runaway switch group name",
            ),
        ];
        for (variant, literal) in literals {
            assert_eq!(&variant.to_string(), literal);
        }
    }
}
