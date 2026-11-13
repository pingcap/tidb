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
                        crate::account_policy::locking_epoch_text(
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
