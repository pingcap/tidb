//! Go `spark_rebase.go`: Spark's legacy (Julian calendar) DATE and
//! TIMESTAMP values, rebased to the proleptic Gregorian calendar the way
//! Spark 3 reads files written by Spark 2.x or with `legacyDateTime`.

use crate::spark_rebase_micros_generated::{
    DIFFS, RECORDS, SPARK_LEGACY_DATE_REBASE_DIFFS, SPARK_LEGACY_DATE_REBASE_SWITCH_DAYS, SWITCHES,
};

/// Go `sparkRebaseDefaultTimeZoneID`.
const SPARK_REBASE_DEFAULT_TIME_ZONE_ID: &str = "UTC";
/// Go `sparkAppName`.
const SPARK_APP_NAME: &str = "spark";
/// Go `sparkVersionMetadataKey`.
pub(crate) const SPARK_VERSION_METADATA_KEY: &str = "org.apache.spark.version";
/// Go `sparkTimeZoneMetadataKey`.
const SPARK_TIME_ZONE_METADATA_KEY: &str = "org.apache.spark.timeZone";
/// Go `sparkLegacyDateTimeMetadataKey`.
pub(crate) const SPARK_LEGACY_DATE_TIME_METADATA_KEY: &str = "org.apache.spark.legacyDateTime";
/// Go `sparkLegacyINT96MetadataKey`.
pub(crate) const SPARK_LEGACY_INT96_METADATA_KEY: &str = "org.apache.spark.legacyINT96";
/// Go `sparkDatetimeRebaseCutoff` (Spark 3.0.0).
pub(crate) const SPARK_DATETIME_REBASE_CUTOFF: (i64, i64, i64) = (3, 0, 0);
/// Go `sparkINT96RebaseCutoff` (Spark 3.1.0).
pub(crate) const SPARK_INT96_REBASE_CUTOFF: (i64, i64, i64) = (3, 1, 0);

/// Go `microsPerDay`.
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;
const UNIX_SECONDS_PER_DAY: i64 = 86_400;
/// Go `julianDayOfUnixEpoch`.
pub(crate) const JULIAN_DAY_OF_UNIX_EPOCH: i64 = 2_440_588;
const JULIAN_DAY_NUMBER_CONVERSION_OFFSET: i64 = 32_082;
const JULIAN_DAYS_PER_FOUR_YEAR_CYCLE: i64 = 1_461;
const JULIAN_MONTH_CONVERSION_FACTOR: i64 = 153;
const JULIAN_CALENDAR_YEAR_OFFSET: i64 = 4_800;

/// Go `metadata.AppVersion`, the part the rebase reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppVersion {
    pub app: String,
    pub version: (i64, i64, i64),
}

impl AppVersion {
    /// Go `metadata.NewAppVersion`: `<app> version <major>.<minor>.<patch>...`.
    pub(crate) fn parse(created_by: &str) -> Option<Self> {
        let lower = created_by.to_lowercase();
        let at = lower.find(" version")?;
        let app = created_by[..at].trim().to_lowercase();
        let rest = created_by[at + " version".len()..].trim_start();
        let version_text: String = rest.chars().take_while(|c| !c.is_whitespace() && *c != '(').collect();
        let mut parts = version_text.split(['.', '-']).map(|p| {
            let digits: String = p.chars().take_while(char::is_ascii_digit).collect();
            digits.parse::<i64>().unwrap_or(0)
        });
        let version = (parts.next().unwrap_or(0), parts.next().unwrap_or(0), parts.next().unwrap_or(0));
        Some(Self { app, version })
    }

    /// Go `AppVersion.LessThan` against a Spark version.
    fn less_than(&self, cutoff: (i64, i64, i64)) -> bool {
        self.app == SPARK_APP_NAME && self.version < cutoff
    }
}

/// The file's key-value metadata lookup and `created_by`.
pub(crate) struct FileInfo<'a> {
    pub kv: Vec<(&'a str, Option<&'a str>)>,
    pub created_by: Option<&'a str>,
}

impl FileInfo<'_> {
    fn find(&self, key: &str) -> Option<Option<&str>> {
        self.kv.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
    }
}

/// Go `sparkVersionFromMetadata`.
fn spark_version_from_metadata(info: &FileInfo<'_>) -> Option<AppVersion> {
    if let Some(version) = info.find(SPARK_VERSION_METADATA_KEY) {
        let version = version.unwrap_or("").trim();
        if version.is_empty() {
            return None;
        }
        let app = AppVersion::parse(&format!("{SPARK_APP_NAME} version {version}"))?;
        return (app.app == SPARK_APP_NAME).then_some(app);
    }
    info.created_by.and_then(AppVersion::parse)
}

/// Go `sparkJulianGregorianRebaseMicrosIndex`.
fn rebase_index(time_zone_id: &str) -> Option<(u32, u16)> {
    if time_zone_id.is_empty() {
        return None;
    }
    RECORDS
        .binary_search_by(|(name, _, _)| (*name).cmp(time_zone_id))
        .ok()
        .map(|i| (RECORDS[i].1, RECORDS[i].2))
}

/// Go `sparkRebaseTimeZoneID`: the zone to rebase in, or empty when the
/// file needs no rebase.
fn spark_rebase_time_zone_id(info: &FileInfo<'_>, cutoff: (i64, i64, i64), legacy_key: &str, fallback: &str) -> String {
    let mut legacy = info.find(legacy_key).is_some();
    if !legacy {
        legacy = spark_version_from_metadata(info).is_some_and(|v| v.less_than(cutoff));
    }
    if !legacy {
        return String::new();
    }
    if let Some(Some(tz)) = info.find(SPARK_TIME_ZONE_METADATA_KEY) {
        let tz = tz.trim();
        if rebase_index(tz).is_some() {
            return tz.to_owned();
        }
    }
    if rebase_index(fallback).is_some() {
        return fallback.to_owned();
    }
    SPARK_REBASE_DEFAULT_TIME_ZONE_ID.to_owned()
}

/// Go `time.Date(year, month, day, 0, 0, 0, 0, UTC).Unix()`, normalizing an
/// out-of-range day the way Go does.
fn unix_seconds_of_date(year: i64, month: i64, day: i64) -> i64 {
    let first = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, 1).expect("a valid month");
    let date = first + chrono::Duration::days(day - 1);
    date.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp()
}

/// Go `julianDayNumberToDate`.
fn julian_day_number_to_date(jdn: i64) -> (i64, i64, i64) {
    let c = jdn + JULIAN_DAY_NUMBER_CONVERSION_OFFSET;
    let d = (4 * c + 3) / JULIAN_DAYS_PER_FOUR_YEAR_CYCLE;
    let e = c - (JULIAN_DAYS_PER_FOUR_YEAR_CYCLE * d) / 4;
    let m = (5 * e + 2) / JULIAN_MONTH_CONVERSION_FACTOR;
    let day = e - (JULIAN_MONTH_CONVERSION_FACTOR * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = d - JULIAN_CALENDAR_YEAR_OFFSET + m / 10;
    (year, month, day)
}

/// Go `rebaseJulianToGregorianDays`.
pub(crate) fn rebase_julian_to_gregorian_days(days: i64) -> i64 {
    if days < SPARK_LEGACY_DATE_REBASE_SWITCH_DAYS[0] {
        let (year, month, day) = julian_day_number_to_date(days + JULIAN_DAY_OF_UNIX_EPOCH);
        return unix_seconds_of_date(year, month, day) / UNIX_SECONDS_PER_DAY;
    }
    let mut i = SPARK_LEGACY_DATE_REBASE_SWITCH_DAYS.len();
    while i > 1 && days < SPARK_LEGACY_DATE_REBASE_SWITCH_DAYS[i - 1] {
        i -= 1;
    }
    days + SPARK_LEGACY_DATE_REBASE_DIFFS[i - 1]
}

/// Go `sparkRebaseMicrosLookup`.
#[derive(Clone, Debug, Default)]
pub(crate) struct SparkRebaseMicrosLookup {
    pub time_zone_id: String,
    switches: &'static [i64],
    diffs: &'static [i64],
}

/// Go `legacyTimestampRebaseCutoffMicros`: 1900-01-01 UTC.
const LEGACY_TIMESTAMP_REBASE_CUTOFF_MICROS: i64 = -2_208_988_800_000_000;

impl SparkRebaseMicrosLookup {
    /// Go `sparkRebaseMicrosFromMetadata`.
    pub(crate) fn from_metadata(info: &FileInfo<'_>, cutoff: (i64, i64, i64), legacy_key: &str, fallback: &str) -> Result<Self, String> {
        let tz = spark_rebase_time_zone_id(info, cutoff, legacy_key, fallback);
        if tz.is_empty() {
            return Ok(Self::default());
        }
        Self::new(&tz)
    }

    /// Go `newSparkRebaseMicrosLookup`.
    fn new(time_zone_id: &str) -> Result<Self, String> {
        let (offset, length) = rebase_index(time_zone_id)
            .ok_or_else(|| format!("unknown Spark legacy timestamp rebase timezone {time_zone_id:?}"))?;
        let (offset, length) = (offset as usize, length as usize);
        if length == 0 {
            return Err(format!("empty Spark legacy timestamp rebase table for timezone {time_zone_id:?}"));
        }
        Ok(Self {
            time_zone_id: time_zone_id.to_owned(),
            switches: &SWITCHES[offset..offset + length],
            diffs: &DIFFS[offset..offset + length],
        })
    }

    /// Go `rebase`.
    pub(crate) fn rebase(&self, micros: i64) -> Result<i64, String> {
        if micros >= LEGACY_TIMESTAMP_REBASE_CUTOFF_MICROS {
            return Ok(micros);
        }
        if self.switches.is_empty() {
            return Err(format!("empty Spark legacy timestamp rebase table for timezone {:?}", self.time_zone_id));
        }
        if micros < self.switches[0] {
            return Ok(rebase_before_switch(micros, self.switches[0], self.diffs[0]));
        }
        let mut i = self.switches.len();
        while i > 1 && micros < self.switches[i - 1] {
            i -= 1;
        }
        Ok(micros + self.diffs[i - 1])
    }
}

/// Go `floorDivInt64`.
pub(crate) fn floor_div(x: i64, y: i64) -> i64 {
    let q = x / y;
    let r = x % y;
    if r != 0 && ((r < 0) != (y < 0)) {
        q - 1
    } else {
        q
    }
}

/// Go `floorModInt64`.
pub(crate) fn floor_mod(x: i64, y: i64) -> i64 {
    x - floor_div(x, y) * y
}

/// Go `rebaseSparkJulianToGregorianMicrosBeforeSwitch`.
fn rebase_before_switch(micros: i64, first_switch: i64, first_diff: i64) -> i64 {
    let julian_common_era_start_micros = SPARK_LEGACY_DATE_REBASE_SWITCH_DAYS[0] * MICROS_PER_DAY;
    let gregorian_common_era_start_micros = unix_seconds_of_date(1, 1, 1) * 1_000_000;
    let source_offset = julian_common_era_start_micros - first_switch;
    let target_offset = gregorian_common_era_start_micros - first_switch - first_diff;
    let local_micros = micros + source_offset;
    let local_days = floor_div(local_micros, MICROS_PER_DAY);
    let micros_of_day = floor_mod(local_micros, MICROS_PER_DAY);
    let (year, month, day) = julian_day_number_to_date(local_days + JULIAN_DAY_OF_UNIX_EPOCH);
    let gregorian_local_micros = unix_seconds_of_date(year, month, day) * 1_000_000 + micros_of_day;
    gregorian_local_micros - target_offset
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_div_matches_go() {
        assert_eq!(floor_div(-1, MICROS_PER_DAY), -1);
        assert_eq!(floor_div(7, 2), 3);
        assert_eq!(floor_div(-7, 2), -4);
        assert_eq!(floor_mod(-7, 2), 1);
        assert_eq!(floor_div(7, -2), -4);
    }

    #[test]
    fn app_version() {
        assert_eq!(
            AppVersion::parse("parquet-mr version 1.10.1 (build abc)"),
            Some(AppVersion { app: "parquet-mr".into(), version: (1, 10, 1) })
        );
        assert!(AppVersion::parse("spark version 2.4.0").unwrap().less_than(SPARK_DATETIME_REBASE_CUTOFF));
        assert!(!AppVersion::parse("spark version 3.3.0").unwrap().less_than(SPARK_DATETIME_REBASE_CUTOFF));
    }
}
