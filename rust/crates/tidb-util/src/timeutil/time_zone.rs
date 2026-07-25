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

//! Transcreation of Go `pkg/util/timeutil/time_zone.go`.

use std::fmt;
use std::str::FromStr;
use std::sync::{Once, RwLock};

use chrono::{Local, Offset, TimeZone as _, Utc};
use chrono_tz::Tz;

use super::ERR_UNKNOWN_TIME_ZONE;
use tidb_error::terror::TerrorError;

/// The Rust shape of Go's `*time.Location`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeZone {
    /// Go `time.Local` — the process-local zone (`"System"` to MySQL).
    Local,
    /// An IANA zone (Go `time.LoadLocation`), backed by `chrono-tz`.
    Named(Tz),
    /// Go `time.FixedZone(name, offset)`.
    Fixed {
        /// The zone's display name; empty for pure-offset zones.
        name: String,
        /// Seconds east of UTC.
        offset_secs: i32,
    },
}

/// Error mirroring the source's `fmt.Errorf` failures (`GetSystemTZ`,
/// `LoadLocation`); the SQL-typed unknown-timezone error is
/// [`super::ERR_UNKNOWN_TIME_ZONE`].
#[derive(Debug, Clone)]
pub struct TimeZoneError(pub String);

impl fmt::Display for TimeZoneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TimeZoneError {}

/// Go's package-level `systemTZ` (an `atomic.String`, initialized "System").
static SYSTEM_TZ: RwLock<Option<String>> = RwLock::new(None);
static SET_SYS_TZ_ONCE: Once = Once::new();

fn system_tz_load() -> String {
    SYSTEM_TZ
        .read()
        .unwrap()
        .clone()
        .unwrap_or_else(|| "System".to_string())
}

/// Stores `systemTZ` directly, bypassing the once-guard — the equivalent of
/// the Go tests' direct `systemTZ.Store` (crate-visible for tests).
pub(crate) fn store_system_tz(name: &str) {
    *SYSTEM_TZ.write().unwrap() = Some(name.to_string());
}

/// Sets `systemTZ` by the value loaded from `mysql.tidb`; only the first call
/// takes effect (Go `SetSystemTZ` with `sync.Once`).
pub fn set_system_tz(name: &str) {
    SET_SYS_TZ_ONCE.call_once(|| store_system_tz(name));
}

/// Gets `systemTZ`; an error when it is not properly set (Go `GetSystemTZ`).
pub fn get_system_tz() -> Result<String, TimeZoneError> {
    let tz = system_tz_load();
    if tz == "System" || tz.is_empty() {
        return Err(TimeZoneError(
            "variable `systemTZ` is not properly set".to_string(),
        ));
    }
    Ok(tz)
}

/// Reads one symlink step for the path — unlike full resolution, which chases
/// links to the end (Go `inferOneStepLinkForPath`).
fn infer_one_step_link_for_path(path: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return std::fs::read_link(path);
    }
    Ok(path.to_path_buf())
}

/// Gets the IANA name from a zoneinfo path (Go `inferTZNameFromFileName`),
/// including the macOS Mojave `zoneinfo.default` layout.
fn infer_tz_name_from_file_name(path: &str) -> Result<String, TimeZoneError> {
    const SUBSTR: &str = "zoneinfo";
    const SUBSTR_MOJAVE: &str = "zoneinfo.default";

    if let Some(idx) = path.find(SUBSTR_MOJAVE) {
        return Ok(path[idx + SUBSTR_MOJAVE.len() + 1..].to_string());
    }
    if let Some(idx) = path.find(SUBSTR) {
        return Ok(path[idx + SUBSTR.len() + 1..].to_string());
    }
    Err(TimeZoneError(format!("path {path} is not supported")))
}

/// Reads the system timezone from `$TZ`, then the `/etc/localtime` symlink;
/// falls back to `"UTC"` (Go `InferSystemTZ`). Exported for bootstrap only,
/// like the source.
pub fn infer_system_tz() -> String {
    match std::env::var("TZ") {
        Err(std::env::VarError::NotPresent) => {
            // No $TZ: consult /etc/localtime.
            match std::fs::canonicalize("/etc/localtime") {
                Ok(path) => {
                    let mut path_str = path.to_string_lossy().to_string();
                    if path_str.contains("posixrules") {
                        match infer_one_step_link_for_path(std::path::Path::new("/etc/localtime")) {
                            Ok(one_step) => path_str = one_step.to_string_lossy().to_string(),
                            Err(err) => {
                                tracing::error!(%err, "locate timezone files failed");
                                return String::new();
                            }
                        }
                    }
                    match infer_tz_name_from_file_name(&path_str) {
                        Ok(name) => return name,
                        Err(err) => tracing::error!(%err, "infer timezone failed"),
                    }
                }
                Err(err) => tracing::error!(%err, "locate timezone files failed"),
            }
        }
        Ok(tz) if !tz.is_empty() && tz != "UTC" && Tz::from_str(&tz).is_ok() => {
            return tz;
        }
        _ => {} // $TZ="" or "UTC" (or an unloadable name) means UTC.
    }
    "UTC".to_string()
}

/// Loads a [`TimeZone`] by IANA name (Go `LoadLocation`; `"System"` is the
/// local zone).
pub fn load_location(name: &str) -> Result<TimeZone, TimeZoneError> {
    if name == "System" {
        return Ok(TimeZone::Local);
    }
    Tz::from_str(name)
        .map(TimeZone::Named)
        .map_err(|_| TimeZoneError(format!("invalid name for timezone {name}")))
}

/// Returns TiDB's global timezone location (Go `SystemLocation`), the local
/// zone if `systemTZ` doesn't resolve.
pub fn system_location() -> TimeZone {
    load_location(&system_tz_load()).unwrap_or(TimeZone::Local)
}

/// Returns the timezone name and its offset in seconds at the present moment
/// (Go `Zone`); the local zone reports as `"System"` for MySQL compatibility.
pub fn zone(loc: &TimeZone) -> (String, i64) {
    match loc {
        TimeZone::Local => (
            "System".to_string(),
            i64::from(Local::now().offset().fix().local_minus_utc()),
        ),
        TimeZone::Named(tz) => {
            let offset = tz
                .from_utc_datetime(&Utc::now().naive_utc())
                .offset()
                .fix()
                .local_minus_utc();
            (tz.name().to_string(), i64::from(offset))
        }
        TimeZone::Fixed { name, offset_secs } => (name.clone(), i64::from(*offset_secs)),
    }
}

/// Returns the zone name, or `"+08:00"`-style offset text when the name is
/// empty (Go `ZoneName`). As in the source, a *named* fixed zone returns its
/// name directly, which may not be re-parsable by [`parse_time_zone`].
pub fn zone_name(loc: &TimeZone) -> String {
    let (name, offset) = zone(loc);
    if !name.is_empty() {
        return name;
    }
    let (sign, offset) = if offset < 0 {
        ('-', -offset)
    } else {
        ('+', offset)
    };
    let hours = offset / 3600;
    let minutes = offset % 3600 / 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

/// Constructs a timezone by name when set (daylight saving handled by the
/// named zone), otherwise by the offset in seconds east of UTC (Go
/// `ConstructTimeZone`).
pub fn construct_time_zone(name: &str, offset_secs: i32) -> Result<TimeZone, TimeZoneError> {
    if !name.is_empty() {
        return load_location(name);
    }
    Ok(TimeZone::Fixed {
        name: String::new(),
        offset_secs,
    })
}

/// Tests whether `now` is between `start` and `end`, comparing only the UTC
/// hour:minute and handling windows that cross midnight (Go
/// `WithinDayTimePeriod`).
pub fn within_day_time_period(
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> bool {
    use chrono::Timelike;
    let minutes = |t: chrono::DateTime<Utc>| i64::from(t.hour()) * 60 + i64::from(t.minute());
    let (start, end, now) = (minutes(start), minutes(end), minutes(now));
    // for cases like from 00:00 to 06:00
    if end - start >= 0 {
        now - start >= 0 && now - end <= 0
    } else {
        // for cases like from 22:00 to 06:00
        now - end <= 0 || now - start >= 0
    }
}

/// Parses a time-zone string: `SYSTEM`, an IANA name, or a `'+10:00'`-style
/// offset within `[-12:59, +14:00]` (Go `ParseTimeZone`).
pub fn parse_time_zone(s: &str) -> Result<TimeZone, TerrorError> {
    if s.eq_ignore_ascii_case("SYSTEM") {
        return Ok(system_location());
    }

    if let Ok(tz) = Tz::from_str(s) {
        return Ok(TimeZone::Named(tz));
    }

    if let Some(rest) = s.strip_prefix('+').or_else(|| s.strip_prefix('-')) {
        if let Some(secs) = parse_duration_hms(rest) {
            let negative = s.starts_with('-');
            let limit = if negative {
                12 * 3600 + 59 * 60
            } else {
                14 * 3600
            };
            if secs > limit {
                return Err(unknown_time_zone(s));
            }
            let offset = if negative {
                -(secs as i32)
            } else {
                secs as i32
            };
            return Ok(TimeZone::Fixed {
                name: String::new(),
                offset_secs: offset,
            });
        }
    }

    Err(unknown_time_zone(s))
}

fn unknown_time_zone(s: &str) -> TerrorError {
    ERR_UNKNOWN_TIME_ZONE.generate_with_stack(format!("Unknown or incorrect time zone: '{s}'"))
}

/// The `[H]H[:MM[:SS]]` subset of Go `types.ParseDuration` that timezone
/// offsets exercise; returns total seconds.
fn parse_duration_hms(s: &str) -> Option<i64> {
    let mut parts = s.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = match parts.next() {
        Some(m) => m.parse().ok()?,
        None => 0,
    };
    let seconds: i64 = match parts.next() {
        Some(sec) => sec.parse().ok()?,
        None => 0,
    };
    if parts.next().is_some() || minutes >= 60 || seconds >= 60 || hours < 0 {
        return None;
    }
    Some(hours * 3600 + minutes * 60 + seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // $TZ and the system-tz global are process-wide; serialize the tests that
    // touch them (Go runs its package tests in one process too).
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // Go `TestGetTZNameFromFileName`.
    #[test]
    fn tz_name_from_file_name() {
        assert_eq!(
            infer_tz_name_from_file_name("/usr/share/zoneinfo/Asia/Shanghai").unwrap(),
            "Asia/Shanghai"
        );
        assert_eq!(
            infer_tz_name_from_file_name("/usr/share/zoneinfo.default/Asia/Shanghai").unwrap(),
            "Asia/Shanghai"
        );
        assert!(infer_tz_name_from_file_name("/nonsense/path").is_err());
    }

    // Go `TestLocal`.
    #[test]
    fn local_inference() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY-adjacent: env mutation is process-global; serialized above.
        std::env::set_var("TZ", "Asia/Shanghai");
        store_system_tz(&infer_system_tz());
        assert_eq!(system_tz_load(), "Asia/Shanghai");
        assert_eq!(zone(&system_location()).0, "Asia/Shanghai");

        std::env::set_var("TZ", "UTC");
        store_system_tz(&infer_system_tz());
        assert_eq!(zone(&system_location()).0, "UTC");

        std::env::set_var("TZ", "");
        store_system_tz(&infer_system_tz());
        assert_eq!(zone(&system_location()).0, "UTC");
        std::env::remove_var("TZ");
    }

    // Go `TestInferOneStepLinkForPath`.
    #[test]
    fn one_step_link() {
        let dir = std::env::temp_dir();
        let l1 = dir.join("tidbrs_testlink1");
        let l2 = dir.join("tidbrs_testlink2");
        let l3 = dir.join("tidbrs_testlink3");
        let _ = std::fs::remove_file(&l3);
        let _ = std::fs::remove_file(&l2);
        let _ = std::fs::remove_file(&l1);
        std::fs::File::create(&l1).unwrap();
        std::os::unix::fs::symlink(&l1, &l2).unwrap();
        std::os::unix::fs::symlink(&l2, &l3).unwrap();

        // One step resolves l3 -> l2 only.
        assert_eq!(infer_one_step_link_for_path(&l3).unwrap(), l2);
        // Full resolution reaches l1.
        assert!(std::fs::canonicalize(&l3)
            .unwrap()
            .to_string_lossy()
            .contains("tidbrs_testlink1"));

        let _ = std::fs::remove_file(&l3);
        let _ = std::fs::remove_file(&l2);
        let _ = std::fs::remove_file(&l1);
    }

    // Go `TestParseTimeZone`.
    #[test]
    fn parse_time_zone_cases() {
        let _guard = ENV_LOCK.lock().unwrap();
        store_system_tz("Asia/Tokyo");

        let cases: &[(&str, i64)] = &[
            ("SYSTEM", 9 * 3600),
            ("system", 9 * 3600),
            ("Asia/Shanghai", 8 * 3600),
            ("Pacific/Honolulu", -10 * 3600),
            ("-07:00", -7 * 3600),
            ("+02:00", 2 * 3600),
        ];
        for (name, want_offset) in cases {
            let loc = parse_time_zone(name).unwrap_or_else(|e| panic!("{name}: {e}"));
            let (_, offset) = zone(&loc);
            assert_eq!(offset, *want_offset, "{name}");
        }

        // Invalid name fails with ERR_UNKNOWN_TIME_ZONE's identity.
        let err = parse_time_zone("aa").unwrap_err();
        assert_eq!(err.code(), ERR_UNKNOWN_TIME_ZONE.code());

        // Offsets outside [-12:59, +14:00] are rejected.
        assert!(parse_time_zone("+14:01").is_err());
        assert!(parse_time_zone("-13:00").is_err());
        assert!(parse_time_zone("+14:00").is_ok());
        assert!(parse_time_zone("-12:59").is_ok());
    }

    // Go `TestZoneName`.
    #[test]
    fn zone_name_cases() {
        assert_eq!(zone_name(&TimeZone::Named(Tz::UTC)), "UTC");
        assert_eq!(
            zone_name(&TimeZone::Fixed {
                name: String::new(),
                offset_secs: 8 * 3600 + 30 * 60
            }),
            "+08:30"
        );
        assert_eq!(
            zone_name(&TimeZone::Fixed {
                name: String::new(),
                offset_secs: -(6 * 3600 + 15 * 60)
            }),
            "-06:15"
        );
        assert_eq!(
            zone_name(&TimeZone::Fixed {
                name: String::new(),
                offset_secs: 0
            }),
            "+00:00"
        );
        assert_eq!(
            zone_name(&TimeZone::Fixed {
                name: "UTC+8".to_string(),
                offset_secs: 8 * 3600
            }),
            "UTC+8"
        );
    }

    // Go `TestConstructTimeZone`: a fixed zone shifts wall time by its offset;
    // a named zone ignores the offset argument.
    #[test]
    fn construct_time_zone_cases() {
        use chrono::TimeZone as _;

        let to_utc = |loc: &TimeZone, y: i32, mo: u32, d: u32, h: u32| -> chrono::DateTime<Utc> {
            let naive = chrono::NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_opt(h, 0, 0)
                .unwrap();
            match loc {
                TimeZone::Fixed { offset_secs, .. } => {
                    let off = chrono::FixedOffset::east_opt(*offset_secs).unwrap();
                    off.from_local_datetime(&naive).unwrap().with_timezone(&Utc)
                }
                TimeZone::Named(tz) => tz.from_local_datetime(&naive).unwrap().with_timezone(&Utc),
                TimeZone::Local => unreachable!(),
            }
        };

        let loc = construct_time_zone("", 8 * 3600).unwrap();
        assert_eq!(
            to_utc(&loc, 2018, 8, 15, 20),
            Utc.with_ymd_and_hms(2018, 8, 15, 12, 0, 0).unwrap()
        );

        let loc = construct_time_zone("", -8 * 3600).unwrap();
        assert_eq!(
            to_utc(&loc, 2018, 8, 15, 12),
            Utc.with_ymd_and_hms(2018, 8, 15, 20, 0, 0).unwrap()
        );

        let loc = construct_time_zone("", 0).unwrap();
        assert_eq!(
            to_utc(&loc, 2018, 8, 15, 20),
            Utc.with_ymd_and_hms(2018, 8, 15, 20, 0, 0).unwrap()
        );

        // The offset argument is ignored when a name is given.
        let loc = construct_time_zone("Asia/Shanghai", 23 * 3600).unwrap();
        assert_eq!(
            to_utc(&loc, 2018, 8, 15, 20),
            Utc.with_ymd_and_hms(2018, 8, 15, 12, 0, 0).unwrap()
        );
    }

    // `WithinDayTimePeriod`'s two window shapes (uncovered by Go's tests).
    #[test]
    fn within_day_time_period_cases() {
        use chrono::TimeZone as _;
        let at = |h: u32, m: u32| Utc.with_ymd_and_hms(2020, 1, 1, h, m, 0).unwrap();

        // Same-day window 00:00-06:00.
        assert!(within_day_time_period(at(0, 0), at(6, 0), at(3, 0)));
        assert!(!within_day_time_period(at(0, 0), at(6, 0), at(7, 0)));

        // Midnight-crossing window 22:00-06:00.
        assert!(within_day_time_period(at(22, 0), at(6, 0), at(23, 0)));
        assert!(within_day_time_period(at(22, 0), at(6, 0), at(3, 0)));
        assert!(!within_day_time_period(at(22, 0), at(6, 0), at(12, 0)));
    }

    // `GetSystemTZ`/`SetSystemTZ` contract.
    #[test]
    fn system_tz_set_once() {
        let _guard = ENV_LOCK.lock().unwrap();
        store_system_tz("System");
        assert!(get_system_tz().is_err());
        set_system_tz("Asia/Shanghai");
        // Later set_system_tz calls are ignored (sync.Once).
        set_system_tz("UTC");
        assert_eq!(get_system_tz().unwrap(), "Asia/Shanghai");
    }
}
