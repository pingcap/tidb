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

//! Stored account policy shared by the snapshot reader and Session registry.

use chrono::{Local, TimeZone, Utc};

/// Go's Password_locking policy and runtime state from mysql.user JSON.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PasswordLocking {
    /// Consecutive failure threshold; zero disables tracking.
    pub failed_login_attempts: i64,
    /// Lock duration in days; -1 means unbounded.
    pub password_lock_time_days: i64,
    /// Current failure count.
    pub failed_login_count: i64,
    /// Whether failed logins locked the account.
    pub auto_account_locked: bool,
    /// Original lock epoch in Unix seconds.
    pub auto_locked_last_changed: i64,
}

impl PasswordLocking {
    /// Go enables tracking only when both options are nonzero.
    pub const fn tracking_enabled(&self) -> bool {
        self.failed_login_attempts != 0 && self.password_lock_time_days != 0
    }

    /// Duration spelling used by error 3955.
    pub fn lock_days_text(&self) -> String {
        if self.password_lock_time_days == -1 {
            "unlimited".into()
        } else {
            self.password_lock_time_days.to_string()
        }
    }

    /// Decode Go-authored JSON, preserving the epoch instead of starting a new lock.
    pub fn from_attributes(attributes: Option<&str>) -> Result<Option<Self>, String> {
        let Some(attributes) = attributes else {
            return Ok(None);
        };
        let value: serde_json::Value =
            serde_json::from_str(attributes).map_err(|error| error.to_string())?;
        let Some(locking) = value.get("Password_locking") else {
            return Ok(None);
        };
        let integer = |name: &str| -> Result<i64, String> {
            match locking.get(name) {
                None => Ok(0),
                Some(value) => value
                    .as_i64()
                    .or_else(|| value.as_u64().map(|n| n as i64))
                    .ok_or_else(|| format!("Password_locking.{name} is not an integer")),
            }
        };
        let epoch = match locking.get("auto_locked_last_changed") {
            None => 0,
            Some(value) => parse_unix_date(value.as_str().ok_or("lock epoch is not a string")?)?,
        };
        Ok(Some(Self {
            failed_login_attempts: integer("failed_login_attempts")?.clamp(0, i16::MAX as i64),
            password_lock_time_days: integer("password_lock_time_days")?.clamp(-1, i16::MAX as i64),
            failed_login_count: integer("failed_login_count")?,
            auto_account_locked: locking
                .get("auto_account_locked")
                .and_then(serde_json::Value::as_str)
                == Some("Y"),
            auto_locked_last_changed: epoch,
        }))
    }
}

fn process_local_zone() -> &'static tidb_util::timeutil::TimeZone {
    static ZONE: std::sync::LazyLock<tidb_util::timeutil::TimeZone> =
        std::sync::LazyLock::new(|| {
            tidb_util::timeutil::load_location(&tidb_util::timeutil::infer_system_tz())
                .unwrap_or(tidb_util::timeutil::TimeZone::Local)
        });
    &ZONE
}

fn parse_unix_date(value: &str) -> Result<i64, String> {
    parse_unix_date_in_zone(value, process_local_zone())
}

fn parse_unix_date_in_zone(
    value: &str,
    zone: &tidb_util::timeutil::TimeZone,
) -> Result<i64, String> {
    // Go ignores a valid weekday and resolves the abbreviation against Local;
    // an unknown abbreviation has offset zero. UTC always has offset zero.
    let parts: Vec<_> = value.split_whitespace().collect();
    if parts.len() != 6 || !["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].contains(&parts[0]) {
        return Err(format!("invalid Go UnixDate: {value}"));
    }
    let calendar = format!("{} {} {} {}", parts[1], parts[2], parts[3], parts[5]);
    let naive = chrono::NaiveDateTime::parse_from_str(&calendar, "%b %d %H:%M:%S %Y")
        .map_err(|error| error.to_string())?;
    match zone {
        tidb_util::timeutil::TimeZone::Named(zone) => {
            for instant in [
                zone.from_local_datetime(&naive).earliest(),
                zone.from_local_datetime(&naive).latest(),
            ]
            .into_iter()
            .flatten()
            {
                if instant.format("%Z").to_string() == parts[4] {
                    return Ok(instant.timestamp());
                }
            }
        }
        tidb_util::timeutil::TimeZone::Local => {
            for instant in [
                Local.from_local_datetime(&naive).earliest(),
                Local.from_local_datetime(&naive).latest(),
            ]
            .into_iter()
            .flatten()
            {
                if instant.format("%Z").to_string() == parts[4] {
                    return Ok(instant.timestamp());
                }
            }
        }
        tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } if name == parts[4] => {
            return Ok(naive.and_utc().timestamp() - i64::from(*offset_secs))
        }
        _ => {}
    }
    Ok(naive.and_utc().timestamp())
}

/// Go UnixDate in the process-local named zone, retaining the original instant.
/// UTC remains a portable fallback when the native zone name is unavailable.
pub fn locking_epoch_text(epoch: i64) -> String {
    let instant = Utc
        .timestamp_opt(epoch, 0)
        .single()
        .expect("valid account lock epoch");
    match process_local_zone() {
        tidb_util::timeutil::TimeZone::Named(zone) => instant
            .with_timezone(zone)
            .format("%a %b %e %H:%M:%S %Z %Y")
            .to_string(),
        _ => instant.format("%a %b %e %H:%M:%S UTC %Y").to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_policy_clamps_options_without_resetting_count_or_epoch() {
        let json = r#"{"Password_locking":{"failed_login_attempts":99999,"password_lock_time_days":-9,"failed_login_count":4,"auto_account_locked":"Y","auto_locked_last_changed":"Sat Feb  3 04:05:06 UTC 2001"},"metadata":{"comment":"keep"}}"#;
        let locking = PasswordLocking::from_attributes(Some(json))
            .unwrap()
            .unwrap();
        assert_eq!(locking.failed_login_attempts, 32767);
        assert_eq!(locking.password_lock_time_days, -1);
        assert_eq!(locking.failed_login_count, 4);
        assert!(locking.auto_account_locked);
        assert_eq!(locking.auto_locked_last_changed, 981_173_106);
        assert_eq!(
            parse_unix_date(&locking_epoch_text(locking.auto_locked_last_changed)).unwrap(),
            locking.auto_locked_last_changed
        );
    }

    #[test]
    fn absent_state_is_zero_and_unknown_zone_uses_go_zero_offset() {
        assert_eq!(PasswordLocking::from_attributes(None).unwrap(), None);
        assert_eq!(PasswordLocking::from_attributes(Some("{}")).unwrap(), None);
        let policy = PasswordLocking::from_attributes(Some(
            r#"{"Password_locking":{"failed_login_attempts":3,"password_lock_time_days":7}}"#,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(policy.auto_locked_last_changed, 0);
        assert_eq!(policy.failed_login_count, 0);
        assert!(!policy.auto_account_locked);
        assert_eq!(
            parse_unix_date("Sat Feb  3 04:05:06 XYZ 2001").unwrap(),
            981_173_106
        );
    }

    #[test]
    fn named_local_zone_resolves_its_abbreviation_only() {
        let tokyo = tidb_util::timeutil::load_location("Asia/Tokyo").unwrap();
        assert_eq!(
            parse_unix_date_in_zone("Sat Feb  3 04:05:06 JST 2001", &tokyo).unwrap(),
            981_140_706
        );
        assert_eq!(
            parse_unix_date_in_zone("Sat Feb  3 04:05:06 XYZ 2001", &tokyo).unwrap(),
            981_173_106
        );
    }

    #[test]
    fn malformed_lock_epoch_fails_instead_of_disabling_policy() {
        assert!(PasswordLocking::from_attributes(Some(
            r#"{"Password_locking":{"auto_locked_last_changed":"broken"}}"#
        ))
        .is_err());
    }
}
