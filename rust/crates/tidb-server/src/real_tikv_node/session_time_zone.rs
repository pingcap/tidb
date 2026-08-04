//! The `time_zone` a real-TiKV session reads and writes in.
//!
//! One value serves both directions of the round trip: its display name goes
//! into the DAG request's `TimeZoneName` field, and the same zone's offset
//! converts every write's `TIMESTAMP` literal to UTC -- so both sides see the
//! zone Go's `SessionVars.Location()` would. Only fixed offsets and the bare
//! `UTC`/`SYSTEM` spellings are supported, because this node threads in no
//! IANA timezone database and approximating a named zone silently would be a
//! wrong answer rather than a missing feature.

/// A real-TiKV session's `time_zone` value: a display name for the DAG
/// request's `TimeZoneName` field, and the same zone's offset in seconds east
/// of UTC. Only fixed offsets and the bare `UTC`/`SYSTEM` spellings are
/// supported — this node carries no IANA timezone database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RealTiKvSessionTimeZone {
    pub(crate) name: String,
    pub(crate) offset_secs: i32,
}

impl Default for RealTiKvSessionTimeZone {
    fn default() -> Self {
        Self {
            name: "UTC".to_owned(),
            offset_secs: 0,
        }
    }
}

impl RealTiKvSessionTimeZone {
    /// This value as the shared [`tidb_datatype::SessionTimeZone`], which is
    /// what derives the DAG request's `TimeZoneName`/`TimeZoneOffset` pair
    /// (`SessionTimeZone::dag_zone`, Go `timeutil.Zone`).
    ///
    /// `SYSTEM` resolves to `UTC` here rather than being passed through as
    /// the written word: Go sends `Zone(SystemLocation())`, the RESOLVED
    /// system zone's name, and this node's system zone IS UTC -- that is
    /// what [`Self::parse`]'s `offset_secs: 0` says. Passing `"SYSTEM"`
    /// through would name a zone no zone database can load, which is the
    /// same failure the offset spellings had.
    pub(crate) fn zone(&self) -> tidb_datatype::SessionTimeZone {
        let system = self.name.eq_ignore_ascii_case("SYSTEM");
        tidb_datatype::SessionTimeZone::Fixed {
            name: if system {
                "UTC".to_owned()
            } else {
                self.name.clone()
            },
            offset_secs: self.offset_secs,
        }
    }

    /// Parses `SET time_zone = <value>`'s source-observable subset: `SYSTEM`,
    /// `UTC`, and fixed `+HH:MM`/`-HH:MM` offsets. Named IANA zones are
    /// refused rather than silently approximated, matching this node's
    /// generally-UTC-only temporal seed.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("SYSTEM") || value.eq_ignore_ascii_case("UTC") {
            return Some(Self {
                name: value.to_owned(),
                offset_secs: 0,
            });
        }
        let offset_secs = parse_fixed_tz_offset(value)?;
        Some(Self {
            name: value.to_owned(),
            offset_secs,
        })
    }
}

/// Parses a fixed UTC offset (`+HH:MM`/`-HH:MM`, e.g. `'+05:00'`, `'-08:00'`)
/// into whole seconds east of UTC.
fn parse_fixed_tz_offset(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hh: i32 = s.get(1..3)?.parse().ok()?;
    let mm: i32 = s.get(4..6)?.parse().ok()?;
    Some(sign * (hh * 3600 + mm * 60))
}

/// Recognizes `SET [SESSION] time_zone = <value>` / `SET @@time_zone = <value>`
/// (case-insensitively; `GLOBAL` is left unmatched, so it falls through to this
/// node's ordinary unsupported-statement handling rather than silently
/// changing session state) and returns the unquoted, un-lowercased value text.
pub(crate) fn parse_set_time_zone(sql: &str) -> Option<&str> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let lower = trimmed.to_ascii_lowercase();
    let mut rest = lower.strip_prefix("set")?.trim_start();
    rest = rest.strip_prefix("session").map_or(rest, str::trim_start);
    rest = rest
        .strip_prefix("@@session.")
        .or_else(|| rest.strip_prefix("@@"))
        .map_or(rest, str::trim_start);
    let rest = rest.strip_prefix("time_zone")?.trim_start();
    let rest = rest.strip_prefix('=')?;
    let value_lower = rest.trim();
    if value_lower.is_empty() {
        return None;
    }
    // `lower` is ASCII-only wherever it overlaps `trimmed`'s SQL keywords, so
    // the byte offset of the value's start is identical in both strings;
    // slicing `trimmed` at that offset recovers the value's original case.
    let start = trimmed.len() - value_lower.len();
    let value = trimmed[start..].trim();
    let unquoted = if value.len() >= 2
        && ((value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('"') && value.ends_with('"')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    };
    Some(unquoted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_time_zone_recognizes_the_source_observable_forms() {
        assert_eq!(
            parse_set_time_zone("SET time_zone='+05:00'"),
            Some("+05:00")
        );
        assert_eq!(
            parse_set_time_zone("set time_zone = '-08:00';"),
            Some("-08:00")
        );
        assert_eq!(
            parse_set_time_zone("SET SESSION time_zone = 'UTC'"),
            Some("UTC")
        );
        assert_eq!(
            parse_set_time_zone("SET @@time_zone = 'SYSTEM'"),
            Some("SYSTEM")
        );
        assert_eq!(
            parse_set_time_zone("SET @@session.time_zone = '+00:00'"),
            Some("+00:00")
        );
        // Unquoted and case-preserved inside the value.
        assert_eq!(parse_set_time_zone("SET time_zone=SYSTEM"), Some("SYSTEM"));
        // Not a `time_zone` assignment at all.
        assert_eq!(parse_set_time_zone("SET autocommit = 0"), None);
        assert_eq!(parse_set_time_zone("SELECT 1"), None);
    }

    /// Go `ConstructDAGReq` stamps `timeutil.Zone(SessionVars.Location())`,
    /// and `Zone` returns `loc.String()` as the NAME. An offset zone is
    /// `time.FixedZone("", ofst)` (`timeutil.ParseTimeZone`'s `+HH:MM`
    /// branch), whose `String()` is EMPTY -- TiKV then falls back to the
    /// offset, which is the only half it can use. Measured against Go on this
    /// branch (`timeutil.Zone(timeutil.ParseTimeZone(s))`, system TZ
    /// `Asia/Shanghai`):
    ///
    /// ```text
    /// 'Asia/Shanghai' -> name "Asia/Shanghai" offset 28800
    /// '+05:00'        -> name ""              offset 18000
    /// '-08:00'        -> name ""              offset -28800
    /// 'UTC'           -> name "UTC"           offset 0
    /// '+00:00'        -> name ""              offset 0
    /// 'SYSTEM'        -> name "Asia/Shanghai" offset 28800
    /// ```
    ///
    /// Note the last two rows: `'+00:00'` is NOT `"UTC"` -- it is an offset
    /// zone that happens to be zero -- and `SYSTEM` sends the RESOLVED system
    /// zone's name, which for this node (whose system zone is UTC) is `UTC`.
    #[test]
    fn an_offset_zone_stamps_an_empty_dag_name_and_a_named_one_stamps_its_name() {
        let dag = |value: &str| {
            RealTiKvSessionTimeZone::parse(value)
                .expect("the probe value parses")
                .zone()
                .dag_zone()
        };
        assert_eq!(dag("+05:00"), (String::new(), 18_000));
        assert_eq!(dag("-08:00"), (String::new(), -28_800));
        assert_eq!(dag("+00:00"), (String::new(), 0));
        assert_eq!(dag("UTC"), ("UTC".to_owned(), 0));
        assert_eq!(dag("SYSTEM"), ("UTC".to_owned(), 0));
    }

    #[test]
    fn real_tikv_session_time_zone_parses_fixed_offsets_and_refuses_named_zones() {
        assert_eq!(
            RealTiKvSessionTimeZone::parse("+05:00"),
            Some(RealTiKvSessionTimeZone {
                name: "+05:00".to_owned(),
                offset_secs: 18_000,
            })
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("-12:00"),
            Some(RealTiKvSessionTimeZone {
                name: "-12:00".to_owned(),
                offset_secs: -43_200,
            })
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("UTC"),
            Some(RealTiKvSessionTimeZone {
                name: "UTC".to_owned(),
                offset_secs: 0,
            })
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("SYSTEM"),
            Some(RealTiKvSessionTimeZone {
                name: "SYSTEM".to_owned(),
                offset_secs: 0,
            })
        );
        // No IANA timezone database is threaded in, so a named zone is
        // refused rather than silently approximated.
        assert_eq!(RealTiKvSessionTimeZone::parse("Asia/Shanghai"), None);
        assert_eq!(RealTiKvSessionTimeZone::parse("not-a-zone"), None);
    }
}
