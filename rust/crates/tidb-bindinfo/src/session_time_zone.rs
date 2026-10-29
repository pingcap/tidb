//! The `time_zone` a real-TiKV session reads and writes in.
//!
//! One value serves both directions of the round trip: its name and current
//! offset go into the DAG request, and the same DST-aware zone converts write
//! literals. Parsing is delegated to `tidb_timeutil::parse_time_zone`,
//! the package owner of Go `timeutil.ParseTimeZone`'s accepted domain.

/// A real-TiKV session's resolved `time_zone`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RealTiKvSessionTimeZone {
    zone: tidb_datatype::SessionTimeZone,
}

impl Default for RealTiKvSessionTimeZone {
    fn default() -> Self {
        Self::from_timeutil(tidb_timeutil::system_location())
    }
}

impl RealTiKvSessionTimeZone {
    fn from_timeutil(zone: tidb_timeutil::TimeZone) -> Self {
        let zone = match zone {
            tidb_timeutil::TimeZone::Local => tidb_datatype::SessionTimeZone::Local,
            tidb_timeutil::TimeZone::Named(zone) => {
                tidb_datatype::SessionTimeZone::Named(zone)
            }
            tidb_timeutil::TimeZone::Fixed { name, offset_secs } => {
                tidb_datatype::SessionTimeZone::Fixed { name, offset_secs }
            }
        };
        Self { zone }
    }

    /// The shared zone used by reads and writes.
    pub fn zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone.clone()
    }

    /// Parses the complete Go `timeutil.ParseTimeZone` domain.
    pub fn parse(value: &str) -> Result<Self, tidb_terror::TerrorError> {
        Ok(Self::from_timeutil(tidb_timeutil::parse_time_zone(
            value,
        )?))
    }
}

#[cfg(test)]
pub fn time_zone_sql_error(
    error: tidb_terror::TerrorError,
) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    tidb_store_driver_error::sql_query_error::SqlQueryError::from(tidb_terror::MysqlError::from_terror(
        &error,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let system = tidb_timeutil::system_location();
        assert_eq!(dag("SYSTEM"), tidb_timeutil::zone(&system));
    }

    #[test]
    fn real_tikv_session_time_zone_uses_the_source_parser() {
        assert_eq!(
            RealTiKvSessionTimeZone::parse("+05:00")
                .unwrap()
                .zone()
                .dag_zone(),
            (String::new(), 18_000)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("-12:00")
                .unwrap()
                .zone()
                .dag_zone(),
            (String::new(), -43_200)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("UTC")
                .unwrap()
                .zone()
                .dag_zone(),
            ("UTC".to_owned(), 0)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("Asia/Shanghai")
                .unwrap()
                .zone()
                .dag_zone(),
            ("Asia/Shanghai".to_owned(), 28_800)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("not-a-zone")
                .expect_err("an invalid name must be rejected")
                .to_sql_error()
                .code,
            1298
        );
    }

    #[test]
    fn real_tikv_session_time_zone_accepts_the_go_parse_time_zone_domain() {
        let shanghai = RealTiKvSessionTimeZone::parse("Asia/Shanghai")
            .expect("Go timeutil.ParseTimeZone accepts named IANA zones");
        assert_eq!(shanghai.zone().dag_zone().0, "Asia/Shanghai");

        let compact = RealTiKvSessionTimeZone::parse("+2")
            .expect("Go parses offsets with MySQL duration grammar");
        assert_eq!(compact.zone().dag_zone(), (String::new(), 2));

        let error = time_zone_sql_error(
            RealTiKvSessionTimeZone::parse("not-a-zone").expect_err("invalid zone"),
        );
        assert_eq!(error.code, 1298);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(
            error.message,
            "Unknown or incorrect time zone: 'not-a-zone'"
        );
    }

    #[test]
    fn fresh_session_uses_the_resolved_system_location() {
        let default = RealTiKvSessionTimeZone::default().zone().dag_zone();
        let system = tidb_timeutil::system_location();
        assert_eq!(default, tidb_timeutil::zone(&system));
    }
}
