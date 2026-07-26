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

//! `pkg/meta/model/resource_group.go`: resource-group settings metadata.

use std::fmt::Write as _;

use tidb_ast::{
    priority_value_to_name, RunawayActionType, RunawayWatchType, MEDIUM_PRIORITY_VALUE,
};

use crate::schema_state::SchemaState;
use crate::setting_builder::{
    write_setting_duration_ms, write_setting_integer, write_setting_item, write_setting_string,
};

/// Go `unlimitedRURate`: the RU-rate sentinel meaning "unlimited"
/// (`math.MaxInt32`).
const UNLIMITED_RU_RATE: u64 = i32::MAX as u64;

/// Go `ResourceGroupRunawaySettings`: the runaway-query limits of a group.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceGroupRunawaySettings {
    /// The execution-time limit, in milliseconds.
    pub exec_elapsed_time_ms: u64,
    /// The processed-keys limit.
    pub processed_keys: i64,
    /// The request-unit limit.
    pub request_unit: i64,
    /// The action taken when a query runs away.
    pub action: RunawayActionType,
    /// The group to switch to (for the switch-group action).
    pub switch_group_name: String,
    /// How runaway queries are watched.
    pub watch_type: RunawayWatchType,
    /// The watch duration, in milliseconds.
    pub watch_duration_ms: i64,
}

/// Go `ResourceGroupBackgroundSettings`: the background-task settings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceGroupBackgroundSettings {
    /// The task types treated as background.
    pub job_types: Vec<String>,
    /// The utilization limit for background tasks.
    pub resource_util_limit: u64,
}

/// Go `ResourceGroupSettings`: the settings of a resource group.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceGroupSettings {
    /// The request units per second.
    pub ru_rate: u64,
    /// The scheduling priority.
    pub priority: u64,
    /// The CPU limit.
    pub cpu_limiter: String,
    /// The I/O read-bandwidth limit.
    pub io_read_bandwidth: String,
    /// The I/O write-bandwidth limit.
    pub io_write_bandwidth: String,
    /// The burst limit (`-1` unlimited, `-2` moderated).
    pub burst_limit: i64,
    /// The runaway-query settings, if any.
    pub runaway: Option<Box<ResourceGroupRunawaySettings>>,
    /// The background-task settings, if any.
    pub background: Option<Box<ResourceGroupBackgroundSettings>>,
}

impl ResourceGroupSettings {
    /// Go `NewResourceGroupSettings`: the defaults (medium priority).
    #[must_use]
    pub fn new() -> Self {
        ResourceGroupSettings {
            priority: MEDIUM_PRIORITY_VALUE,
            ..Default::default()
        }
    }

    /// Go `GetBurstLimitAdjusted`: `-1` when the RU rate is unlimited, else
    /// the configured burst limit.
    #[must_use]
    pub fn get_burst_limit_adjusted(&self) -> i64 {
        if self.ru_rate == UNLIMITED_RU_RATE {
            -1
        } else {
            self.burst_limit
        }
    }

    /// Go `Adjust`: with a limited RU rate and a non-negative burst limit,
    /// the burst limit tracks the RU rate.
    pub fn adjust(&mut self) {
        if self.ru_rate != UNLIMITED_RU_RATE && self.burst_limit >= 0 {
            self.burst_limit = self.ru_rate as i64;
        }
    }
}

impl std::fmt::Display for ResourceGroupSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reproduces Go's ResourceGroupSettings.String() -- the top-level
        // items are separated by ", ", but the QUERY_LIMIT inner items use the
        // default single-space separator.
        let sep = Some(", ");
        let mut sb = String::new();
        if self.ru_rate != 0 {
            write_setting_integer(&mut sb, "RU_PER_SEC", self.ru_rate, sep);
        }
        write_setting_item(
            &mut sb,
            &format!("PRIORITY={}", priority_value_to_name(self.priority)),
            sep,
        );
        if !self.cpu_limiter.is_empty() {
            write_setting_string(&mut sb, "CPU", &self.cpu_limiter, sep);
        }
        if !self.io_read_bandwidth.is_empty() {
            write_setting_string(&mut sb, "IO_READ_BANDWIDTH", &self.io_read_bandwidth, sep);
        }
        if !self.io_write_bandwidth.is_empty() {
            write_setting_string(&mut sb, "IO_WRITE_BANDWIDTH", &self.io_write_bandwidth, sep);
        }
        match self.burst_limit {
            -2 => write_setting_item(&mut sb, "BURSTABLE(MODERATED)", sep),
            -1 => write_setting_item(&mut sb, "BURSTABLE(UNLIMITED)", sep),
            _ => {}
        }
        if let Some(runaway) = &self.runaway {
            sb.push_str(", QUERY_LIMIT=(");
            let mut first_param = true;
            if runaway.exec_elapsed_time_ms > 0 {
                let _ = write!(
                    sb,
                    "EXEC_ELAPSED=\"{}\"",
                    crate::go_duration::format_go_duration_ms(runaway.exec_elapsed_time_ms as i64)
                );
                first_param = false;
            }
            if runaway.processed_keys > 0 {
                if !first_param {
                    sb.push(' ');
                }
                let _ = write!(sb, "PROCESSED_KEYS={}", runaway.processed_keys);
                first_param = false;
            }
            if runaway.request_unit > 0 {
                if !first_param {
                    sb.push(' ');
                }
                let _ = write!(sb, "RU={}", runaway.request_unit);
            }
            if runaway.action == RunawayActionType::SwitchGroup {
                write_setting_item(
                    &mut sb,
                    &format!(
                        "ACTION={}({})",
                        runaway.action.sql(),
                        runaway.switch_group_name
                    ),
                    None,
                );
            } else {
                write_setting_item(&mut sb, &format!("ACTION={}", runaway.action.sql()), None);
            }
            if runaway.watch_type != RunawayWatchType::None {
                write_setting_item(
                    &mut sb,
                    &format!("WATCH={}", runaway.watch_type.sql()),
                    None,
                );
                if runaway.watch_duration_ms > 0 {
                    write_setting_duration_ms(&mut sb, "DURATION", runaway.watch_duration_ms, None);
                } else {
                    write_setting_item(&mut sb, "DURATION=UNLIMITED", None);
                }
            }
            sb.push(')');
        }
        if let Some(background) = &self.background {
            sb.push_str(", BACKGROUND=(");
            let mut first = true;
            if !background.job_types.is_empty() {
                let _ = write!(sb, "TASK_TYPES='{}'", background.job_types.join(","));
                first = false;
            }
            if background.resource_util_limit > 0 {
                if !first {
                    sb.push_str(", ");
                }
                let _ = write!(sb, "UTILIZATION_LIMIT={}", background.resource_util_limit);
            }
            sb.push(')');
        }
        f.write_str(&sb)
    }
}

/// Go `ResourceGroupInfo`: a resource group (its settings plus identity/state).
#[derive(Clone, Debug, Default)]
pub struct ResourceGroupInfo {
    /// The group settings (Go's embedded `*ResourceGroupSettings`).
    pub settings: Option<Box<ResourceGroupSettings>>,
    /// The group ID.
    pub id: i64,
    /// The group name.
    pub name: tidb_ast::CiString,
    /// The online-DDL state of the group object.
    pub state: SchemaState,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_priority() {
        let s = ResourceGroupSettings::new();
        assert_eq!(s.priority, MEDIUM_PRIORITY_VALUE);
        assert_eq!(s.to_string(), "PRIORITY=MEDIUM");
    }

    #[test]
    fn ru_burstable() {
        let s = ResourceGroupSettings {
            ru_rate: 1000,
            priority: 16, // HIGH
            burst_limit: -1,
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "RU_PER_SEC=1000, PRIORITY=HIGH, BURSTABLE(UNLIMITED)"
        );
    }

    #[test]
    fn runaway_string() {
        let s = ResourceGroupSettings {
            priority: MEDIUM_PRIORITY_VALUE,
            runaway: Some(Box::new(ResourceGroupRunawaySettings {
                exec_elapsed_time_ms: 1500,
                action: RunawayActionType::Kill,
                watch_type: RunawayWatchType::None,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "PRIORITY=MEDIUM, QUERY_LIMIT=(EXEC_ELAPSED=\"1.5s\" ACTION=KILL)"
        );

        // Switch-group action and a watch with an unlimited duration.
        let s = ResourceGroupSettings {
            priority: MEDIUM_PRIORITY_VALUE,
            runaway: Some(Box::new(ResourceGroupRunawaySettings {
                processed_keys: 100,
                request_unit: 200,
                action: RunawayActionType::SwitchGroup,
                switch_group_name: "rg2".into(),
                watch_type: RunawayWatchType::Exact,
                watch_duration_ms: 0,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "PRIORITY=MEDIUM, QUERY_LIMIT=(PROCESSED_KEYS=100 RU=200 ACTION=SWITCH_GROUP(rg2) \
             WATCH=EXACT DURATION=UNLIMITED)"
        );
    }

    #[test]
    fn background_string() {
        let s = ResourceGroupSettings {
            priority: MEDIUM_PRIORITY_VALUE,
            background: Some(Box::new(ResourceGroupBackgroundSettings {
                job_types: vec!["br".into(), "lightning".into()],
                resource_util_limit: 30,
            })),
            ..Default::default()
        };
        assert_eq!(
            s.to_string(),
            "PRIORITY=MEDIUM, BACKGROUND=(TASK_TYPES='br,lightning', UTILIZATION_LIMIT=30)"
        );
    }

    #[test]
    fn burst_limit_adjusted_and_adjust() {
        let s = ResourceGroupSettings {
            ru_rate: UNLIMITED_RU_RATE,
            burst_limit: 5,
            ..Default::default()
        };
        assert_eq!(s.get_burst_limit_adjusted(), -1);

        let mut s = ResourceGroupSettings {
            ru_rate: 1000,
            burst_limit: 0,
            ..Default::default()
        };
        s.adjust();
        assert_eq!(s.burst_limit, 1000);

        // Unlimited RU rate leaves the burst limit untouched.
        let mut s = ResourceGroupSettings {
            ru_rate: UNLIMITED_RU_RATE,
            burst_limit: 7,
            ..Default::default()
        };
        s.adjust();
        assert_eq!(s.burst_limit, 7);
    }

    #[test]
    fn info_clone_deep() {
        let info = ResourceGroupInfo {
            settings: Some(Box::new(ResourceGroupSettings::new())),
            ..Default::default()
        };
        let mut cloned = info.clone();
        cloned.id = 9;
        cloned.settings.as_mut().unwrap().ru_rate = 42;
        assert_eq!(info.id, 0);
        assert_eq!(info.settings.as_ref().unwrap().ru_rate, 0);
    }
}
