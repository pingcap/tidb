//! Go `type_converter.go`: Parquet values as TiDB datums, by physical and
//! logical type.

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use tidb_datatype::{CoreTime, Datum, Decimal, MySqlDuration, SessionTimeZone, Time, TimeType};

use crate::spark_rebase::{SparkRebaseMicrosLookup, JULIAN_DAY_OF_UNIX_EPOCH, MICROS_PER_DAY};

/// Go `maximumDecimalBytes`.
const MAXIMUM_DECIMAL_BYTES: usize = 33;

/// Go `schema.TimeUnitType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl TimeUnit {
    fn go_value(self) -> i32 {
        match self {
            Self::Millis => 1,
            Self::Micros => 2,
            Self::Nanos => 3,
        }
    }
}

/// Go `schema.LogicalType`, the variants the parser tells apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LogicalType {
    /// Go `NoLogicalType`.
    None,
    String,
    Map,
    List,
    Enum,
    Decimal { precision: i32, scale: i32 },
    Date,
    Time { adjusted_to_utc: bool, unit: TimeUnit },
    Timestamp { adjusted_to_utc: bool, unit: TimeUnit },
    Int { bit_width: i8, signed: bool },
    /// Go `NullLogicalType` (Thrift `UNKNOWN`).
    Null,
    Json,
    Bson,
    Uuid,
    Interval,
    Float16,
    Variant,
    /// Go `UnknownLogicalType`: a type the reader does not know.
    Unknown(String),
}

impl LogicalType {
    /// Go `LogicalType.String`.
    pub(crate) fn go_string(&self) -> String {
        match self {
            Self::None => "None".into(),
            Self::String => "String".into(),
            Self::Map => "Map".into(),
            Self::List => "List".into(),
            Self::Enum => "Enum".into(),
            Self::Decimal { precision, scale } => format!("Decimal(precision={precision}, scale={scale})"),
            Self::Date => "Date".into(),
            Self::Time { adjusted_to_utc, unit } => {
                format!("Time(isAdjustedToUTC={adjusted_to_utc}, timeUnit={})", unit_name(*unit))
            }
            Self::Timestamp { adjusted_to_utc, unit } => {
                format!("Timestamp(isAdjustedToUTC={adjusted_to_utc}, timeUnit={}, is_from_converted_type=false, force_set_converted_type=false)", unit_name(*unit))
            }
            Self::Int { bit_width, signed } => format!("Int(bitWidth={bit_width}, isSigned={signed})"),
            Self::Null => "Null".into(),
            Self::Json => "JSON".into(),
            Self::Bson => "BSON".into(),
            Self::Uuid => "UUID".into(),
            Self::Interval => "Interval".into(),
            Self::Float16 => "Float16".into(),
            Self::Variant => "Variant".into(),
            Self::Unknown(name) => name.clone(),
        }
    }
}

fn unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Millis => "milliseconds",
        TimeUnit::Micros => "microseconds",
        TimeUnit::Nanos => "nanoseconds",
    }
}

/// Go `parquetColumnType`.
#[derive(Clone, Debug)]
pub(crate) struct ParquetColumnType {
    pub logical_type: LogicalType,
    pub spark_rebase_micros: SparkRebaseMicrosLookup,
}

fn unsupported(logical_type: &LogicalType) -> String {
    format!("unsupported parquet logical type {}", logical_type.go_string())
}

/// Go `types.FromGoTime` then `types.NewTime(t, tp, fsp)`: the local wall
/// clock of `t`, rounded to microseconds by adding 500ns.
fn time_datum<TZ: TimeZone>(t: DateTime<TZ>, kind: TimeType, fsp: i64) -> Result<Datum, String> {
    let t = t + chrono::Duration::nanoseconds(500);
    let core = CoreTime::from_date(
        t.year() as u16,
        t.month() as u8,
        t.day() as u8,
        t.hour() as u8,
        t.minute() as u8,
        t.second() as u8,
        t.nanosecond() % 1_000_000_000 / 1000,
    );
    Time::new(core, kind, fsp).map(Datum::Time).map_err(|e| format!("{e:?}"))
}

fn unix_utc(seconds: i64, nanos: i64) -> Result<DateTime<Utc>, String> {
    let secs = seconds + nanos.div_euclid(1_000_000_000);
    let nanos = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nanos).ok_or_else(|| "parquet time value out of range".to_owned())
}

/// Go `setTimestampDatum`.
fn set_timestamp_datum(t: DateTime<Utc>, loc: &SessionTimeZone, adjusted_to_utc: bool) -> Result<Datum, String> {
    if adjusted_to_utc {
        time_datum(t.with_timezone(loc), TimeType::Timestamp, 6)
    } else {
        time_datum(t, TimeType::Timestamp, 6)
    }
}

/// Go `setTimeAsDuration`.
fn set_time_as_duration(val: i64, unit: TimeUnit, loc: &SessionTimeZone, adjusted_to_utc: bool) -> Result<Datum, String> {
    let (units_per_day, mut nanos) = match unit {
        TimeUnit::Millis => (86_400_000, val.saturating_mul(1_000_000)),
        TimeUnit::Micros => (86_400_000_000, val.saturating_mul(1000)),
        TimeUnit::Nanos => (86_400_000_000_000, val),
    };
    if val < 0 || val >= units_per_day {
        return Err(format!(
            "parquet TIME value {val} is outside the valid range for unit {}",
            unit.go_value()
        ));
    }
    if adjusted_to_utc {
        let local = unix_utc(0, nanos)?.with_timezone(loc);
        nanos = i64::from(local.hour()) * 3_600_000_000_000
            + i64::from(local.minute()) * 60_000_000_000
            + i64::from(local.second()) * 1_000_000_000
            + i64::from(local.nanosecond() % 1_000_000_000);
    }
    // Go `Duration.Round(time.Microsecond)`: halves away from zero.
    let nanos = (nanos + 500) / 1000 * 1000;
    MySqlDuration::from_nanoseconds(nanos, 6).map(Datum::new_duration).map_err(|e| format!("{e:?}"))
}

/// Go `setParquetDecimalFromInt64`.
fn decimal_from_int64(unscaled: i64, scale: i32) -> Result<Datum, String> {
    let (shifted, warning) = Decimal::from_int(unscaled).shift_mysql(-scale);
    if warning.is_some() {
        return Err("parquet decimal value out of range".to_owned());
    }
    Ok(Datum::Decimal(shifted.truncate_to_scale(scale)))
}

/// Go `setDatumFromDecimalByte`.
fn decimal_from_bytes(val: &[u8], scale: i32) -> Result<Datum, String> {
    if val.is_empty() {
        return Err("invalid parquet decimal byte array".to_owned());
    }
    let negative = val[0] & 0x80 != 0;
    let mut start = 0;
    while start < val.len() {
        if (negative && val[start] != 0xff) || (!negative && val[start] != 0x00) {
            break;
        }
        start += 1;
    }
    let start = start.saturating_sub(1);
    let mut val = val[start..].to_vec();
    if val.len() >= MAXIMUM_DECIMAL_BYTES || scale > 81 {
        return Ok(Datum::new_string_datum(get_string_from_parquet_byte(&mut val, scale)));
    }
    let (dec, warning) = Decimal::from_parquet_array(&mut val, scale);
    if warning.is_some() {
        return Err("parquet decimal value out of range".to_owned());
    }
    Ok(Datum::Decimal(dec))
}

/// Go `getStringFromParquetByte`: a decimal too wide for `MyDecimal`, as
/// text.
pub(crate) fn get_string_from_parquet_byte(raw: &mut [u8], scale: i32) -> Vec<u8> {
    const BASE: u64 = 1_000_000_000;
    const BASE_DIGITS: usize = 9;
    let scale = usize::try_from(scale).unwrap_or(0);
    let negative = raw[0] & 0x80 != 0;
    if negative {
        for b in raw.iter_mut() {
            *b = !*b;
        }
        for i in (0..raw.len()).rev() {
            raw[i] = raw[i].wrapping_add(1);
            if raw[i] != 0 {
                break;
            }
        }
    }
    let mut s = Vec::with_capacity(64);
    let mut n_digits = 0;
    let mut start = 0;
    let end = raw.len();
    while start < end && raw[start] == 0 {
        start += 1;
    }
    while start < end {
        let mut rem: u64 = 0;
        for i in start..end {
            let v = (rem << 8) | u64::from(raw[i]);
            let q = v / BASE;
            rem = v % BASE;
            raw[i] = q as u8;
            if q == 0 && i == start {
                start += 1;
            }
        }
        for _ in 0..BASE_DIGITS {
            s.push(b'0' + (rem % 10) as u8);
            n_digits += 1;
            rem /= 10;
            if n_digits == scale {
                s.push(b'.');
            }
            if start == end && rem == 0 {
                break;
            }
        }
    }
    while n_digits < scale + 1 {
        s.push(b'0');
        n_digits += 1;
        if n_digits == scale {
            s.push(b'.');
        }
    }
    if negative {
        s.push(b'-');
    }
    s.reverse();
    s
}

/// Go `arrow.Date32(val).ToTime()` then the date datum.
fn date_datum(days: i64) -> Result<Datum, String> {
    time_datum(unix_utc(days * 86_400, 0)?, TimeType::Date, 0)
}

/// Go `getBoolDataSetter`.
pub(crate) fn bool_datum(val: bool) -> Datum {
    Datum::UInt(u64::from(val))
}

/// Go `getInt32Setter`.
pub(crate) fn int32_datum(col: &ParquetColumnType, loc: &SessionTimeZone, val: i32) -> Result<Datum, String> {
    match &col.logical_type {
        LogicalType::Decimal { scale, .. } => decimal_from_int64(i64::from(val), *scale),
        LogicalType::Date => {
            let mut days = i64::from(val);
            if !col.spark_rebase_micros.time_zone_id.is_empty() {
                days = crate::spark_rebase::rebase_julian_to_gregorian_days(days);
            }
            date_datum(days)
        }
        LogicalType::Time { adjusted_to_utc, unit } => set_time_as_duration(i64::from(val), *unit, loc, *adjusted_to_utc),
        LogicalType::Int { signed, .. } => Ok(if *signed { Datum::Int(i64::from(val)) } else { Datum::UInt(u64::from(val as u32)) }),
        LogicalType::None => Ok(Datum::Int(i64::from(val))),
        other => Err(unsupported(other)),
    }
}

/// Go `rebaseTimestampValue`.
fn rebase_timestamp_value(val: i64, unit: TimeUnit, lookup: &SparkRebaseMicrosLookup) -> Result<i64, String> {
    match unit {
        TimeUnit::Millis => lookup.rebase(val * 1000).map(|v| v / 1000),
        TimeUnit::Micros => lookup.rebase(val),
        TimeUnit::Nanos => Ok(val),
    }
}

/// Go `getInt64Setter`.
pub(crate) fn int64_datum(col: &ParquetColumnType, loc: &SessionTimeZone, val: i64) -> Result<Datum, String> {
    match &col.logical_type {
        LogicalType::Int { signed, .. } => Ok(if *signed { Datum::Int(val) } else { Datum::UInt(val as u64) }),
        LogicalType::None => Ok(Datum::Int(val)),
        LogicalType::Time { adjusted_to_utc, unit } => match unit {
            TimeUnit::Micros | TimeUnit::Nanos => set_time_as_duration(val, *unit, loc, *adjusted_to_utc),
            TimeUnit::Millis => Err(unsupported(&col.logical_type)),
        },
        LogicalType::Timestamp { adjusted_to_utc, unit } => {
            let mut val = val;
            if !col.spark_rebase_micros.time_zone_id.is_empty() {
                val = rebase_timestamp_value(val, *unit, &col.spark_rebase_micros)?;
            }
            let t = match unit {
                TimeUnit::Millis => unix_utc(val.div_euclid(1000), val.rem_euclid(1000) * 1_000_000)?,
                TimeUnit::Micros => unix_utc(val.div_euclid(1_000_000), val.rem_euclid(1_000_000) * 1000)?,
                TimeUnit::Nanos => unix_utc(0, val)?,
            };
            set_timestamp_datum(t, loc, *adjusted_to_utc)
        }
        LogicalType::Decimal { scale, .. } => decimal_from_int64(val, *scale),
        other => Err(unsupported(other)),
    }
}

/// Go `int96ToUnixMicros` / `int96ToGoTime` and `setInt96Data`.
pub(crate) fn int96_datum(col: &ParquetColumnType, loc: &SessionTimeZone, words: [u32; 3]) -> Result<Datum, String> {
    let nanos_of_day = (u64::from(words[1]) << 32 | u64::from(words[0])) as i64;
    let julian_day = i64::from(words[2]);
    let t = if col.spark_rebase_micros.time_zone_id.is_empty() {
        let seconds = (julian_day - JULIAN_DAY_OF_UNIX_EPOCH) * 86_400 + nanos_of_day / 1_000_000_000;
        unix_utc(seconds, nanos_of_day % 1_000_000_000)?
    } else {
        let micros = (julian_day - JULIAN_DAY_OF_UNIX_EPOCH) * MICROS_PER_DAY + nanos_of_day / 1000;
        let rebased = col.spark_rebase_micros.rebase(micros)?;
        unix_utc(rebased.div_euclid(1_000_000), rebased.rem_euclid(1_000_000) * 1000)?
    };
    set_timestamp_datum(t, loc, true)
}

/// Go `setFloat32Data`.
pub(crate) fn float32_datum(val: f32) -> Datum {
    Datum::Float32(f64::from(val))
}

/// Go `setFloat64Data`.
pub(crate) fn float64_datum(val: f64) -> Datum {
    Datum::Real(val)
}

/// Go `getByteArraySetter`.
pub(crate) fn byte_array_datum(col: &ParquetColumnType, val: &[u8]) -> Result<Datum, String> {
    match &col.logical_type {
        LogicalType::None | LogicalType::Unknown(_) | LogicalType::Bson | LogicalType::Json | LogicalType::String | LogicalType::Enum => {
            Ok(Datum::new_string_datum(val.to_vec()))
        }
        LogicalType::Decimal { scale, .. } => decimal_from_bytes(val, *scale),
        other => Err(unsupported(other)),
    }
}

/// Go `getFixedLenByteArraySetter`.
pub(crate) fn fixed_len_byte_array_datum(col: &ParquetColumnType, val: &[u8]) -> Result<Datum, String> {
    match &col.logical_type {
        LogicalType::None
        | LogicalType::Unknown(_)
        | LogicalType::Bson
        | LogicalType::Json
        | LogicalType::String
        | LogicalType::Enum
        | LogicalType::Uuid => Ok(Datum::new_string_datum(val.to_vec())),
        LogicalType::Decimal { scale, .. } => decimal_from_bytes(val, *scale),
        other => Err(unsupported(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestBinaryToDecimalStr`.
    #[test]
    fn test_binary_to_decimal_str() {
        for (bytes, scale, want) in [
            (vec![0x01u8, 0x00], 0, "256"),
            (vec![0xff, 0xff], 2, "-0.01"),
            (vec![0x00, 0x7b], 3, "0.123"),
            (vec![0x30, 0x39], 2, "123.45"),
        ] {
            let mut b = bytes.clone();
            assert_eq!(String::from_utf8(get_string_from_parquet_byte(&mut b, scale)).unwrap(), want, "{bytes:?}");
        }
    }
}
