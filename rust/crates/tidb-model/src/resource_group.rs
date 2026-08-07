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

//! Complete transcreation of `pkg/meta/model/resource_group.go`.

use std::fmt::Write as _;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_ast::{
    priority_value_to_name, CiString, RunawayActionType, RunawayWatchType, MEDIUM_PRIORITY_VALUE,
};

use crate::go_duration::format_go_duration;
use crate::schema_state::SchemaState;
use crate::setting_builder::{write_setting_integer, write_setting_item, write_setting_string};

/// Go `unlimitedRURate`: the RU-rate sentinel meaning unlimited.
pub(crate) const UNLIMITED_RU_RATE: u64 = i32::MAX as u64;

/// An open `int32` representation of Go's `ast.RunawayActionType`.
///
/// `tidb_ast::RunawayActionType` is currently a closed Rust enum. Catalog JSON
/// must nevertheless preserve future/unknown Go ordinals, so the stored model
/// uses this open representation at the persistence boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceGroupRunawayAction(pub i32);

impl ResourceGroupRunawayAction {
    /// Go `ast.RunawayActionNone`.
    pub const NONE: Self = Self(0);
    /// Go `ast.RunawayActionDryRun`.
    pub const DRY_RUN: Self = Self(1);
    /// Go `ast.RunawayActionCooldown`.
    pub const COOLDOWN: Self = Self(2);
    /// Go `ast.RunawayActionKill`.
    pub const KILL: Self = Self(3);
    /// Go `ast.RunawayActionSwitchGroup`.
    pub const SWITCH_GROUP: Self = Self(4);

    fn sql(self) -> &'static str {
        match self {
            Self::DRY_RUN => "DRYRUN",
            Self::COOLDOWN => "COOLDOWN",
            Self::KILL => "KILL",
            Self::SWITCH_GROUP => "SWITCH_GROUP",
            _ => "DRYRUN",
        }
    }
}

impl From<RunawayActionType> for ResourceGroupRunawayAction {
    fn from(value: RunawayActionType) -> Self {
        match value {
            RunawayActionType::None => Self::NONE,
            RunawayActionType::DryRun => Self::DRY_RUN,
            RunawayActionType::Cooldown => Self::COOLDOWN,
            RunawayActionType::Kill => Self::KILL,
            RunawayActionType::SwitchGroup => Self::SWITCH_GROUP,
        }
    }
}

/// An open `int32` representation of Go's `ast.RunawayWatchType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceGroupRunawayWatch(pub i32);

impl ResourceGroupRunawayWatch {
    /// Go `ast.WatchNone`.
    pub const NONE: Self = Self(0);
    /// Go `ast.WatchExact`.
    pub const EXACT: Self = Self(1);
    /// Go `ast.WatchSimilar`.
    pub const SIMILAR: Self = Self(2);
    /// Go `ast.WatchPlan`.
    pub const PLAN: Self = Self(3);

    fn sql(self) -> &'static str {
        match self {
            Self::EXACT => "EXACT",
            Self::SIMILAR => "SIMILAR",
            Self::PLAN => "PLAN",
            _ => "NONE",
        }
    }
}

impl From<RunawayWatchType> for ResourceGroupRunawayWatch {
    fn from(value: RunawayWatchType) -> Self {
        match value {
            RunawayWatchType::None => Self::NONE,
            RunawayWatchType::Exact => Self::EXACT,
            RunawayWatchType::Similar => Self::SIMILAR,
            RunawayWatchType::Plan => Self::PLAN,
        }
    }
}

/// A concurrency-safe representation of a Go pointer shared by a shallow
/// struct copy. Cloning this value preserves pointer identity.
pub struct ResourceGroupShared<T>(Arc<RwLock<T>>);

impl<T> ResourceGroupShared<T> {
    /// Allocates a new shared Go-pointer value.
    pub fn new(value: T) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }

    /// Reads the shared value. Poisoning does not exist in Go, so a panic while
    /// holding the lock does not permanently make the catalog value unusable.
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Mutates the shared value while preserving shallow-clone aliasing.
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reports Go pointer identity.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<T> Clone for ResourceGroupShared<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ResourceGroupShared<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ResourceGroupShared")
            .field(&*self.read())
            .finish()
    }
}

impl<T: Clone + PartialEq> PartialEq for ResourceGroupShared<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        let left = self.read().clone();
        let right = other.read().clone();
        left == right
    }
}

impl<T: Clone + Eq> Eq for ResourceGroupShared<T> {}

impl<T: Serialize> Serialize for ResourceGroupShared<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.read().serialize(serializer)
    }
}

impl<T> From<T> for ResourceGroupShared<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

/// Go `ResourceGroupRunawaySettings`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ResourceGroupRunawaySettings {
    /// Execution-time limit in milliseconds.
    pub exec_elapsed_time_ms: u64,
    /// Processed-key limit.
    pub processed_keys: i64,
    /// Request-unit limit.
    pub request_unit: i64,
    /// Runaway action, preserving every Go `int32` ordinal.
    pub action: ResourceGroupRunawayAction,
    /// Destination group for `SWITCH_GROUP`.
    pub switch_group_name: String,
    /// Watch type, preserving every Go `int32` ordinal.
    pub watch_type: ResourceGroupRunawayWatch,
    /// Watch duration in milliseconds.
    pub watch_duration_ms: i64,
}

/// Go `ResourceGroupBackgroundSettings`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ResourceGroupBackgroundSettings {
    /// Background task types. `None` is a nil Go slice; `Some(vec![])` is an
    /// allocated empty slice, which is observably different in JSON.
    pub job_types: Option<Vec<String>>,
    /// Resource-utilization percentage limit.
    #[serde(rename = "utilization_limit")]
    pub resource_util_limit: u64,
}

/// Go `ResourceGroupSettings`.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct ResourceGroupSettings {
    /// Request units per second.
    #[serde(rename = "ru_per_sec")]
    pub ru_rate: u64,
    /// Scheduling priority.
    pub priority: u64,
    /// CPU limit.
    #[serde(rename = "cpu_limit")]
    pub cpu_limiter: String,
    /// I/O read-bandwidth limit.
    pub io_read_bandwidth: String,
    /// I/O write-bandwidth limit.
    pub io_write_bandwidth: String,
    /// Burst limit (`-1` unlimited, `-2` moderated).
    pub burst_limit: i64,
    /// Shared Go pointer to runaway settings.
    pub runaway: Option<ResourceGroupShared<ResourceGroupRunawaySettings>>,
    /// Shared Go pointer to background settings.
    pub background: Option<ResourceGroupShared<ResourceGroupBackgroundSettings>>,
}

impl Clone for ResourceGroupSettings {
    fn clone(&self) -> Self {
        Self {
            ru_rate: self.ru_rate,
            priority: self.priority,
            cpu_limiter: self.cpu_limiter.clone(),
            io_read_bandwidth: self.io_read_bandwidth.clone(),
            io_write_bandwidth: self.io_write_bandwidth.clone(),
            burst_limit: self.burst_limit,
            runaway: self.runaway.clone(),
            background: self.background.clone(),
        }
    }
}

impl ResourceGroupSettings {
    /// Go `NewResourceGroupSettings`: zero settings with medium priority.
    #[must_use]
    pub fn new() -> Self {
        Self {
            priority: MEDIUM_PRIORITY_VALUE,
            ..Self::default()
        }
    }

    /// Go `GetBurstLimitAdjusted`.
    #[must_use]
    pub fn get_burst_limit_adjusted(&self) -> i64 {
        if self.ru_rate == UNLIMITED_RU_RATE {
            -1
        } else {
            self.burst_limit
        }
    }

    /// Go `Adjust`, including Go's wrapping `uint64` to `int64` conversion.
    pub fn adjust(&mut self) {
        if self.ru_rate != UNLIMITED_RU_RATE && self.burst_limit >= 0 {
            self.burst_limit = self.ru_rate as i64;
        }
    }
}

fn go_duration_from_u64_ms(milliseconds: u64) -> String {
    format_go_duration((milliseconds as i64).wrapping_mul(1_000_000))
}

fn go_duration_from_i64_ms(milliseconds: i64) -> String {
    format_go_duration(milliseconds.wrapping_mul(1_000_000))
}

impl std::fmt::Display for ResourceGroupSettings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let separator = Some(", ");
        let mut builder = String::new();
        if self.ru_rate != 0 {
            write_setting_integer(&mut builder, "RU_PER_SEC", self.ru_rate, separator);
        }
        write_setting_item(
            &mut builder,
            &format!("PRIORITY={}", priority_value_to_name(self.priority)),
            separator,
        );
        if !self.cpu_limiter.is_empty() {
            write_setting_string(&mut builder, "CPU", &self.cpu_limiter, separator);
        }
        if !self.io_read_bandwidth.is_empty() {
            write_setting_string(
                &mut builder,
                "IO_READ_BANDWIDTH",
                &self.io_read_bandwidth,
                separator,
            );
        }
        if !self.io_write_bandwidth.is_empty() {
            write_setting_string(
                &mut builder,
                "IO_WRITE_BANDWIDTH",
                &self.io_write_bandwidth,
                separator,
            );
        }
        match self.burst_limit {
            -2 => write_setting_item(&mut builder, "BURSTABLE(MODERATED)", separator),
            -1 => write_setting_item(&mut builder, "BURSTABLE(UNLIMITED)", separator),
            _ => {}
        }
        if let Some(runaway) = &self.runaway {
            let runaway = runaway.read();
            builder.push_str(", QUERY_LIMIT=(");
            let mut first_parameter = true;
            if runaway.exec_elapsed_time_ms > 0 {
                let _ = write!(
                    builder,
                    "EXEC_ELAPSED=\"{}\"",
                    go_duration_from_u64_ms(runaway.exec_elapsed_time_ms)
                );
                first_parameter = false;
            }
            if runaway.processed_keys > 0 {
                if !first_parameter {
                    builder.push(' ');
                }
                let _ = write!(builder, "PROCESSED_KEYS={}", runaway.processed_keys);
                first_parameter = false;
            }
            if runaway.request_unit > 0 {
                if !first_parameter {
                    builder.push(' ');
                }
                let _ = write!(builder, "RU={}", runaway.request_unit);
            }
            if runaway.action == ResourceGroupRunawayAction::SWITCH_GROUP {
                write_setting_item(
                    &mut builder,
                    &format!(
                        "ACTION={}({})",
                        runaway.action.sql(),
                        runaway.switch_group_name
                    ),
                    None,
                );
            } else {
                write_setting_item(
                    &mut builder,
                    &format!("ACTION={}", runaway.action.sql()),
                    None,
                );
            }
            if runaway.watch_type != ResourceGroupRunawayWatch::NONE {
                write_setting_item(
                    &mut builder,
                    &format!("WATCH={}", runaway.watch_type.sql()),
                    None,
                );
                if runaway.watch_duration_ms > 0 {
                    write_setting_string(
                        &mut builder,
                        "DURATION",
                        &go_duration_from_i64_ms(runaway.watch_duration_ms),
                        None,
                    );
                } else {
                    write_setting_item(&mut builder, "DURATION=UNLIMITED", None);
                }
            }
            builder.push(')');
        }
        if let Some(background) = &self.background {
            let background = background.read();
            builder.push_str(", BACKGROUND=(");
            let mut first = true;
            if background
                .job_types
                .as_ref()
                .is_some_and(|jobs| !jobs.is_empty())
            {
                let jobs = background.job_types.as_ref().expect("checked as present");
                let _ = write!(builder, "TASK_TYPES='{}'", jobs.join(","));
                first = false;
            }
            if background.resource_util_limit > 0 {
                if !first {
                    builder.push_str(", ");
                }
                let _ = write!(
                    builder,
                    "UTILIZATION_LIMIT={}",
                    background.resource_util_limit
                );
            }
            builder.push(')');
        }
        formatter.write_str(&builder)
    }
}

/// Go `ResourceGroupInfo`.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ResourceGroupInfo {
    /// Go's anonymous `*ResourceGroupSettings`. Its fields flatten into JSON.
    pub settings: Option<Box<ResourceGroupSettings>>,
    /// Group ID.
    pub id: i64,
    /// Case-insensitive group name.
    pub name: CiString,
    /// Online-DDL state.
    pub state: SchemaState,
}

impl Clone for ResourceGroupInfo {
    fn clone(&self) -> Self {
        let settings = self
            .settings
            .as_ref()
            .expect("Go ResourceGroupInfo.Clone panics for nil ResourceGroupSettings")
            .clone();
        Self {
            settings: Some(settings),
            id: self.id,
            name: self.name.clone(),
            state: self.state,
        }
    }
}

impl Serialize for ResourceGroupInfo {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map =
            serializer.serialize_map(Some(if self.settings.is_some() { 11 } else { 3 }))?;
        if let Some(settings) = &self.settings {
            map.serialize_entry("ru_per_sec", &settings.ru_rate)?;
            map.serialize_entry("priority", &settings.priority)?;
            map.serialize_entry("cpu_limit", &settings.cpu_limiter)?;
            map.serialize_entry("io_read_bandwidth", &settings.io_read_bandwidth)?;
            map.serialize_entry("io_write_bandwidth", &settings.io_write_bandwidth)?;
            map.serialize_entry("burst_limit", &settings.burst_limit)?;
            map.serialize_entry("runaway", &settings.runaway)?;
            map.serialize_entry("background", &settings.background)?;
        }
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("state", &self.state)?;
        map.end()
    }
}

struct GoCiString(CiString);

impl<'de> Deserialize<'de> for GoCiString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct GoCiStringVisitor;

        impl<'de> Visitor<'de> for GoCiStringVisitor {
            type Value = GoCiString;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Go ast.CIStr JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut original = String::new();
                let mut lowercase = String::new();
                while let Some(key) = map.next_key::<String>()? {
                    if key.eq_ignore_ascii_case("O") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            original = value;
                        }
                    } else if key.eq_ignore_ascii_case("L") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            lowercase = value;
                        }
                    } else {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                }
                let value = serde_json::from_value(serde_json::json!({
                    "O": original,
                    "L": lowercase,
                }))
                .map_err(serde::de::Error::custom)?;
                Ok(GoCiString(value))
            }
        }

        deserializer.deserialize_map(GoCiStringVisitor)
    }
}

fn update_runaway_field<'de, A: MapAccess<'de>>(
    value: &mut ResourceGroupRunawaySettings,
    key: &str,
    map: &mut A,
) -> Result<bool, A::Error> {
    if key.eq_ignore_ascii_case("exec_elapsed_time_ms") {
        if let Some(next) = map.next_value::<Option<u64>>()? {
            value.exec_elapsed_time_ms = next;
        }
    } else if key.eq_ignore_ascii_case("processed_keys") {
        if let Some(next) = map.next_value::<Option<i64>>()? {
            value.processed_keys = next;
        }
    } else if key.eq_ignore_ascii_case("request_unit") {
        if let Some(next) = map.next_value::<Option<i64>>()? {
            value.request_unit = next;
        }
    } else if key.eq_ignore_ascii_case("action") {
        if let Some(next) = map.next_value::<Option<ResourceGroupRunawayAction>>()? {
            value.action = next;
        }
    } else if key.eq_ignore_ascii_case("switch_group_name") {
        if let Some(next) = map.next_value::<Option<String>>()? {
            value.switch_group_name = next;
        }
    } else if key.eq_ignore_ascii_case("watch_type") {
        if let Some(next) = map.next_value::<Option<ResourceGroupRunawayWatch>>()? {
            value.watch_type = next;
        }
    } else if key.eq_ignore_ascii_case("watch_duration_ms") {
        if let Some(next) = map.next_value::<Option<i64>>()? {
            value.watch_duration_ms = next;
        }
    } else {
        return Ok(false);
    }
    Ok(true)
}

impl<'de> Deserialize<'de> for ResourceGroupRunawaySettings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RunawayVisitor;
        impl<'de> Visitor<'de> for RunawayVisitor {
            type Value = ResourceGroupRunawaySettings;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Go ResourceGroupRunawaySettings JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut value = ResourceGroupRunawaySettings::default();
                while let Some(key) = map.next_key::<String>()? {
                    if !update_runaway_field(&mut value, &key, &mut map)? {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }
        deserializer.deserialize_map(RunawayVisitor)
    }
}

fn update_background_field<'de, A: MapAccess<'de>>(
    value: &mut ResourceGroupBackgroundSettings,
    key: &str,
    map: &mut A,
) -> Result<bool, A::Error> {
    if key.eq_ignore_ascii_case("job_types") {
        value.job_types = map.next_value::<Option<Vec<String>>>()?;
    } else if key.eq_ignore_ascii_case("utilization_limit") {
        if let Some(next) = map.next_value::<Option<u64>>()? {
            value.resource_util_limit = next;
        }
    } else {
        return Ok(false);
    }
    Ok(true)
}

impl<'de> Deserialize<'de> for ResourceGroupBackgroundSettings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BackgroundVisitor;
        impl<'de> Visitor<'de> for BackgroundVisitor {
            type Value = ResourceGroupBackgroundSettings;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Go ResourceGroupBackgroundSettings JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut value = ResourceGroupBackgroundSettings::default();
                while let Some(key) = map.next_key::<String>()? {
                    if !update_background_field(&mut value, &key, &mut map)? {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }
        deserializer.deserialize_map(BackgroundVisitor)
    }
}

fn is_settings_key(key: &str) -> bool {
    [
        "ru_per_sec",
        "priority",
        "cpu_limit",
        "io_read_bandwidth",
        "io_write_bandwidth",
        "burst_limit",
        "runaway",
        "background",
    ]
    .iter()
    .any(|candidate| key.eq_ignore_ascii_case(candidate))
}

fn update_settings_field<'de, A: MapAccess<'de>>(
    value: &mut ResourceGroupSettings,
    key: &str,
    map: &mut A,
) -> Result<(), A::Error> {
    if key.eq_ignore_ascii_case("ru_per_sec") {
        if let Some(next) = map.next_value::<Option<u64>>()? {
            value.ru_rate = next;
        }
    } else if key.eq_ignore_ascii_case("priority") {
        if let Some(next) = map.next_value::<Option<u64>>()? {
            value.priority = next;
        }
    } else if key.eq_ignore_ascii_case("cpu_limit") {
        if let Some(next) = map.next_value::<Option<String>>()? {
            value.cpu_limiter = next;
        }
    } else if key.eq_ignore_ascii_case("io_read_bandwidth") {
        if let Some(next) = map.next_value::<Option<String>>()? {
            value.io_read_bandwidth = next;
        }
    } else if key.eq_ignore_ascii_case("io_write_bandwidth") {
        if let Some(next) = map.next_value::<Option<String>>()? {
            value.io_write_bandwidth = next;
        }
    } else if key.eq_ignore_ascii_case("burst_limit") {
        if let Some(next) = map.next_value::<Option<i64>>()? {
            value.burst_limit = next;
        }
    } else if key.eq_ignore_ascii_case("runaway") {
        value.runaway = map
            .next_value::<Option<ResourceGroupRunawaySettings>>()?
            .map(ResourceGroupShared::new);
    } else if key.eq_ignore_ascii_case("background") {
        value.background = map
            .next_value::<Option<ResourceGroupBackgroundSettings>>()?
            .map(ResourceGroupShared::new);
    } else {
        unreachable!("caller checks is_settings_key")
    }
    Ok(())
}

impl<'de> Deserialize<'de> for ResourceGroupSettings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SettingsVisitor;
        impl<'de> Visitor<'de> for SettingsVisitor {
            type Value = ResourceGroupSettings;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Go ResourceGroupSettings JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut value = ResourceGroupSettings::default();
                while let Some(key) = map.next_key::<String>()? {
                    if is_settings_key(&key) {
                        update_settings_field(&mut value, &key, &mut map)?;
                    } else {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }
        deserializer.deserialize_map(SettingsVisitor)
    }
}

impl<'de> Deserialize<'de> for ResourceGroupInfo {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct InfoVisitor;
        impl<'de> Visitor<'de> for InfoVisitor {
            type Value = ResourceGroupInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Go ResourceGroupInfo JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut value = ResourceGroupInfo::default();
                while let Some(key) = map.next_key::<String>()? {
                    if is_settings_key(&key) {
                        let settings = value
                            .settings
                            .get_or_insert_with(|| Box::new(ResourceGroupSettings::default()));
                        update_settings_field(settings, &key, &mut map)?;
                    } else if key.eq_ignore_ascii_case("id") {
                        if let Some(next) = map.next_value::<Option<i64>>()? {
                            value.id = next;
                        }
                    } else if key.eq_ignore_ascii_case("name") {
                        if let Some(next) = map.next_value::<Option<GoCiString>>()? {
                            value.name = next.0;
                        }
                    } else if key.eq_ignore_ascii_case("state") {
                        if let Some(next) = map.next_value::<Option<SchemaState>>()? {
                            value.state = next;
                        }
                    } else {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }
        deserializer.deserialize_map(InfoVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_and_adjustment_boundaries() {
        assert_eq!(ResourceGroupSettings::default().priority, 0);
        assert_eq!(ResourceGroupSettings::new().priority, MEDIUM_PRIORITY_VALUE);

        for (ru_rate, burst_limit, expected_adjusted, expected_stored) in [
            (UNLIMITED_RU_RATE, 0, -1, 0),
            (UNLIMITED_RU_RATE, -2, -1, -2),
            (0, 0, 0, 0),
            (u64::MAX, 0, 0, -1),
            (7, -1, -1, -1),
        ] {
            let mut settings = ResourceGroupSettings {
                ru_rate,
                burst_limit,
                ..ResourceGroupSettings::default()
            };
            assert_eq!(settings.get_burst_limit_adjusted(), expected_adjusted);
            settings.adjust();
            assert_eq!(settings.burst_limit, expected_stored);
        }
    }

    #[test]
    fn clone_is_top_level_copy_with_shared_nested_pointers() {
        let settings = ResourceGroupSettings {
            runaway: Some(
                ResourceGroupRunawaySettings {
                    processed_keys: 1,
                    ..ResourceGroupRunawaySettings::default()
                }
                .into(),
            ),
            background: Some(
                ResourceGroupBackgroundSettings {
                    job_types: Some(vec!["a".to_owned(), "b".to_owned()]),
                    ..ResourceGroupBackgroundSettings::default()
                }
                .into(),
            ),
            ..ResourceGroupSettings::default()
        };
        let cloned = settings.clone();
        assert!(!std::ptr::eq(&settings, &cloned));
        assert!(settings
            .runaway
            .as_ref()
            .unwrap()
            .ptr_eq(cloned.runaway.as_ref().unwrap()));
        assert!(settings
            .background
            .as_ref()
            .unwrap()
            .ptr_eq(cloned.background.as_ref().unwrap()));
        cloned.runaway.as_ref().unwrap().write().processed_keys = 9;
        cloned
            .background
            .as_ref()
            .unwrap()
            .write()
            .job_types
            .as_mut()
            .unwrap()[0] = "changed".to_owned();
        assert_eq!(settings.runaway.as_ref().unwrap().read().processed_keys, 9);
        assert_eq!(
            settings
                .background
                .as_ref()
                .unwrap()
                .read()
                .job_types
                .as_ref()
                .unwrap()[0],
            "changed"
        );

        let info = ResourceGroupInfo {
            settings: Some(Box::new(settings)),
            ..ResourceGroupInfo::default()
        };
        let info_clone = info.clone();
        assert!(!std::ptr::eq(
            info.settings.as_ref().unwrap().as_ref(),
            info_clone.settings.as_ref().unwrap().as_ref()
        ));
        assert!(info
            .settings
            .as_ref()
            .unwrap()
            .runaway
            .as_ref()
            .unwrap()
            .ptr_eq(
                info_clone
                    .settings
                    .as_ref()
                    .unwrap()
                    .runaway
                    .as_ref()
                    .unwrap()
            ));
        assert!(std::panic::catch_unwind(|| ResourceGroupInfo::default().clone()).is_err());
    }
}
