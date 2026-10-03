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

//! Account-owned credential history, shared by CREATE/ALTER/SET/DROP/RENAME.

use super::*;
use chrono::TimeZone;
use tidb_exec::cluster_privilege_load::LoadedPasswordHistory;
use tidb_executor::DriverError;

impl UserRecord {
    pub(super) fn export_attributes(&self) -> Option<String> {
        let mut raw = self.user_attributes.as_deref().map(|text| {
            serde_json::from_str::<serde_json::Value>(text).expect("validated account JSON")
        });
        let original = PasswordLocking::from_attributes(self.user_attributes.as_deref())
            .expect("validated locking JSON");
        if original == self.password_locking {
            return self.user_attributes.clone();
        }
        let value = raw.get_or_insert_with(|| serde_json::json!({}));
        if !value.is_object() {
            *value = serde_json::json!({});
        }
        match self.password_locking {
            None => {
                value.as_object_mut().unwrap().remove("Password_locking");
            }
            Some(locking) => {
                let object = value
                    .as_object_mut()
                    .unwrap()
                    .entry("Password_locking")
                    .or_insert_with(|| serde_json::json!({}));
                if !object.is_object() {
                    *object = serde_json::json!({});
                }
                object["failed_login_attempts"] = locking.failed_login_attempts.into();
                object["password_lock_time_days"] = locking.password_lock_time_days.into();
                let previous = original.unwrap_or_default();
                if locking.failed_login_count != previous.failed_login_count
                    || locking.auto_account_locked != previous.auto_account_locked
                    || locking.auto_locked_last_changed != previous.auto_locked_last_changed
                {
                    object["failed_login_count"] = locking.failed_login_count.into();
                    object["auto_account_locked"] = if locking.auto_account_locked {
                        "Y"
                    } else {
                        "N"
                    }
                    .into();
                    object["auto_locked_last_changed"] =
                        tidb_exec::account_policy::locking_epoch_text(
                            locking.auto_locked_last_changed,
                        )
                        .into();
                }
            }
        }
        // Go removes a sole all-zero locking object to NULL, retaining other metadata.
        if value.as_object().is_some_and(|object| object.is_empty())
            && self.password_locking.is_none()
        {
            None
        } else {
            Some(value.to_string())
        }
    }
}

impl PrivilegeRegistry {
    /// Restore durable policy without invoking ALTER's reset or the current clock.
    pub fn restore_account_policy(
        &self,
        user: &str,
        host: &str,
        count: Option<i64>,
        days: Option<i64>,
        attributes: Option<String>,
        locking: Option<PasswordLocking>,
    ) {
        if let Some(record) = self.lock().get_mut(&(user.into(), host.into())) {
            record.password_reuse_history = count;
            record.password_reuse_time = days;
            record.user_attributes = attributes;
            record.password_locking = locking;
        }
    }

    /// Replace the loaded history as part of a single account snapshot publication.
    pub fn restore_password_history(&self, rows: Vec<LoadedPasswordHistory>) {
        *self.password_history.lock().unwrap() = rows;
    }

    /// Current nullable policy, preserving DEFAULT versus an explicit zero.
    pub fn password_reuse_policy(&self, user: &str, host: &str) -> (Option<i64>, Option<i64>) {
        self.lock()
            .get(&(user.into(), host.into()))
            .map(|record| (record.password_reuse_history, record.password_reuse_time))
            .unwrap_or_default()
    }

    /// Apply only options explicitly present on this statement.
    pub fn set_password_reuse_policy(
        &self,
        user: &str,
        host: &str,
        count: Option<Option<i64>>,
        days: Option<Option<i64>>,
    ) {
        if let Some(record) = self.lock().get_mut(&(user.into(), host.into())) {
            if let Some(count) = count {
                record.password_reuse_history = count;
            }
            if let Some(days) = days {
                record.password_reuse_time = days;
            }
        }
    }

    /// Complete attributes used by the durable bridge and the local table mirror.
    pub fn user_attributes(&self, user: &str, host: &str) -> Option<String> {
        self.lock()
            .get(&(user.into(), host.into()))
            .and_then(UserRecord::export_attributes)
    }

    /// Merge account metadata/secondary-password changes without discarding locking state.
    pub fn merge_user_attributes(
        &self,
        user: &str,
        host: &str,
        patch: serde_json::Value,
        drop_secondary: bool,
    ) {
        if let Some(record) = self.lock().get_mut(&(user.into(), host.into())) {
            let mut value = record
                .export_attributes()
                .as_deref()
                .map(|text| serde_json::from_str::<serde_json::Value>(text).unwrap())
                .unwrap_or_else(|| serde_json::json!({}));
            merge_patch(&mut value, &patch);
            if drop_secondary {
                if let Some(object) = value.as_object_mut() {
                    object.remove("additional_password");
                }
            }
            record.user_attributes =
                if drop_secondary && value.as_object().is_some_and(|object| object.is_empty()) {
                    None
                } else {
                    Some(value.to_string())
                };
        }
    }

    /// Remove this account's history on DROP or successful plugin changes.
    pub fn clear_password_history(&self, user: &str, host: &str) {
        self.password_history
            .lock()
            .unwrap()
            .retain(|row| row.user != user || row.host != host);
    }

    /// History rows for one identity, including their original microsecond timestamps.
    pub fn account_password_history(&self, user: &str, host: &str) -> Vec<LoadedPasswordHistory> {
        self.password_history
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.user == user && row.host == host)
            .cloned()
            .collect()
    }

    /// Check the union of Go's count/time windows, then prune and append atomically.
    /// Call before credential publication; a rejection leaves every history row intact.
    #[allow(clippy::too_many_arguments)]
    pub fn record_password_history(
        &self,
        user: &str,
        host: &str,
        encoded: &str,
        plaintext: Option<&str>,
        plugin: &str,
        count: i64,
        days: i64,
        creating: bool,
        plugin_changed: bool,
        session_zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<(), DriverError> {
        // Go CREATE excludes only auth-token entries. LDAP is additionally
        // exempt in ALTER/SET's checkPasswordReusePolicy, preserving old rows.
        let exempt = plugin.eq_ignore_ascii_case(tidb_mysql::consts::AuthTiDBAuthToken)
            || (!creating
                && [
                    tidb_mysql::consts::AuthLDAPSASL,
                    tidb_mysql::consts::AuthLDAPSimple,
                ]
                .iter()
                .any(|name| plugin.eq_ignore_ascii_case(name)));
        let instant = self.clock.now_precise();
        let now = instant.timestamp();
        let timestamp = tidb_datatype::Time::new(
            tidb_datatype::core_time_from_datetime(instant),
            tidb_datatype::TimeType::Timestamp,
            6,
        )
        .expect("valid timestamp precision");
        let mut history = self.password_history.lock().unwrap();
        let mut account: Vec<_> = if plugin_changed {
            Vec::new()
        } else {
            history
                .iter()
                .filter(|row| row.user == user && row.host == host)
                .cloned()
                .collect()
        };
        account.sort_by_key(|row| row.timestamp.go_raw());
        if !exempt && !encoded.is_empty() {
            // Go getValidTime formats the integer Unix cutoff in process Local,
            // then the TIMESTAMP comparison interprets that wall time in the session zone.
            let cutoff = if days > 0 && days <= i32::MAX as i64 {
                let before = now
                    .saturating_sub(days.saturating_mul(SECONDS_PER_DAY))
                    .max(0);
                let local = chrono::Local
                    .timestamp_opt(before, 0)
                    .single()
                    .expect("valid cutoff")
                    .naive_local();
                session_zone
                    .from_local_datetime(&local)
                    .earliest()
                    .ok_or_else(|| {
                        DriverError::Exec(tidb_executor::ExecError::internal(
                            "invalid password history cutoff in session timezone",
                        ))
                    })?
                    .timestamp()
            } else {
                0
            };
            let epoch = |row: &LoadedPasswordHistory| {
                row.timestamp
                    .core_time()
                    .to_datetime(&chrono::Utc)
                    .map(|time| time.timestamp())
                    .unwrap_or(-62_135_596_800)
            };
            let total = i64::try_from(account.len()).unwrap_or(i64::MAX);
            let all = total <= count || days > i32::MAX as i64;
            if !creating && (count > 0 || days > 0) {
                if !matches!(
                    plugin,
                    "" | tidb_mysql::consts::AuthNativePassword
                        | tidb_mysql::consts::AuthCachingSha2Password
                        | tidb_mysql::consts::AuthTiDBSM3Password
                ) {
                    return Err(DriverError::PluginIsNotLoaded {
                        plugin: plugin.into(),
                    });
                }
                for (index, row) in account.iter().enumerate() {
                    let recent = count > 0 && index >= account.len().saturating_sub(count as usize);
                    if !(all || recent || (days > 0 && epoch(row) >= cutoff)) {
                        continue;
                    }
                    let matches = match plugin {
                        "" | tidb_mysql::consts::AuthNativePassword => {
                            row.password.as_deref() == Some(encoded)
                        }
                        tidb_mysql::consts::AuthCachingSha2Password
                        | tidb_mysql::consts::AuthTiDBSM3Password => {
                            match row.password.as_deref() {
                                None => false,
                                Some(stored) => tidb_parser::auth::check_hashing_password_bytes(
                                    stored.as_bytes(),
                                    plaintext.unwrap_or("").as_bytes(),
                                    plugin,
                                )
                                .map_err(|error| {
                                    DriverError::Exec(tidb_executor::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })?,
                            }
                        }
                        _ => {
                            return Err(DriverError::PluginIsNotLoaded {
                                plugin: plugin.into(),
                            })
                        }
                    };
                    if matches {
                        return Err(DriverError::PasswordInHistory {
                            user: user.into(),
                            host: host.into(),
                        });
                    }
                }
            }
            if !creating && days <= i32::MAX as i64 {
                let mut remaining = total.saturating_sub(count).saturating_add(1).max(0);
                account.retain(|row| {
                    if remaining > 0 && (days == 0 || epoch(row) < cutoff) {
                        remaining -= 1;
                        false
                    } else {
                        true
                    }
                });
            }
            if count > 0 || days > 0 {
                if account.iter().any(|row| row.timestamp == timestamp) {
                    return Err(DriverError::DuplicateEntry {
                        value: format!("{host}-{user}-{timestamp}"),
                        key: "mysql.password_history.PRIMARY".into(),
                    });
                }
                account.push(LoadedPasswordHistory {
                    host: host.into(),
                    user: user.into(),
                    timestamp,
                    password: Some(encoded.into()),
                });
            }
        }
        if plugin_changed || (!exempt && !encoded.is_empty()) {
            history.retain(|row| row.user != user || row.host != host);
            history.extend(account);
        }
        Ok(())
    }
}

fn merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    if let Some(patch) = patch.as_object() {
        if !target.is_object() {
            *target = serde_json::json!({});
        }
        let target = target.as_object_mut().unwrap();
        for (name, value) in patch {
            if value.is_null() {
                target.remove(name);
            } else {
                merge_patch(
                    target
                        .entry(name.clone())
                        .or_insert(serde_json::Value::Null),
                    value,
                );
            }
        }
    } else {
        *target = patch.clone();
    }
}
